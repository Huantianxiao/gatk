package org.broadinstitute.hellbender.engine.spark.datasources;

import com.tom_e_white.squark.HtsjdkReadsRdd;
import com.tom_e_white.squark.HtsjdkReadsRddStorage;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.GATKReadToBDGAlignmentRecordConverter;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;
import org.broadinstitute.hellbender.utils.spark.SparkUtils;
import scala.Tuple2;

import java.io.IOException;

/**
 * ReadsSparkSink writes GATKReads to a file. This code lifts from the HadoopGenomics/Hadoop-BAM
 * read writing code as well as from bigdatagenomics/adam.
 */
public final class ReadsSparkSink {

    private final static Logger logger = LogManager.getLogger(ReadsSparkSink.class);

    /**
     * writeReads writes rddReads to outputFile with header as the file header.
     * @param ctx the JavaSparkContext to write.
     * @param outputFile path to the output bam.
     * @param referenceFile path to the reference. required for cram output, otherwise may be null.
     * @param reads reads to write.
     * @param header the header to put at the top of the files
     * @param format should the output be a single file, sharded, ADAM, etc.
     */
    public static void writeReads(
            final JavaSparkContext ctx, final String outputFile, final String referenceFile, final JavaRDD<GATKRead> reads,
            final SAMFileHeader header, ReadsWriteFormat format) throws IOException {
        writeReads(ctx, outputFile, referenceFile, reads, header, format, 0);
    }

    /**
     * writeReads writes rddReads to outputFile with header as the file header.
     * @param ctx the JavaSparkContext to write.
     * @param outputFile path to the output bam.
     * @param referenceFile path to the reference. required for cram output, otherwise may be null.
     * @param reads reads to write.
     * @param header the header to put at the top of the files
     * @param format should the output be a single file, sharded, ADAM, etc.
     * @param numReducers the number of reducers to use when writing a single file. A value of zero indicates that the default
     *                    should be used.
     */
    public static void writeReads(
            final JavaSparkContext ctx, final String outputFile, final String referenceFile, final JavaRDD<GATKRead> reads,
            final SAMFileHeader header, ReadsWriteFormat format, final int numReducers) throws IOException {

        String absoluteOutputFile = BucketUtils.makeFilePathAbsolute(outputFile);
        String absoluteReferenceFile = referenceFile != null ?
                                        BucketUtils.makeFilePathAbsolute(referenceFile) :
                                        referenceFile;
        ReadsSparkSource.checkCramReference(ctx, absoluteOutputFile, absoluteReferenceFile);

        // The underlying reads are required to be in SAMRecord format in order to be
        // written out, so we convert them to SAMRecord explicitly here. If they're already
        // SAMRecords, this will effectively be a no-op. The SAMRecords will be headerless
        // for efficient serialization.
        // TODO: add header here
        final JavaRDD<SAMRecord> samReads = reads.map(read -> read.convertToSAMRecord(null));

        if (format == ReadsWriteFormat.SINGLE || format == ReadsWriteFormat.SHARDED) {
            HtsjdkReadsRddStorage.FormatWriteOption formatWriteOption = IOUtils.isCramFileName(outputFile)
                    ? HtsjdkReadsRddStorage.FormatWriteOption.CRAM : HtsjdkReadsRddStorage.FormatWriteOption.BAM; // TODO: SAM; format for directory?
            HtsjdkReadsRddStorage.FileCardinalityWriteOption fileCardinalityWriteOption =
                    format == ReadsWriteFormat.SINGLE ? HtsjdkReadsRddStorage.FileCardinalityWriteOption.SINGLE
                            : HtsjdkReadsRddStorage.FileCardinalityWriteOption.MULTIPLE;
            writeReads(ctx, absoluteOutputFile, absoluteReferenceFile, samReads, header, numReducers, formatWriteOption, fileCardinalityWriteOption);
        } else if (format == ReadsWriteFormat.ADAM) {
            writeReadsADAM(ctx, absoluteOutputFile, samReads, header);
        }
    }

    private static void writeReads(
            final JavaSparkContext ctx, final String outputFile, final String referenceFile, final JavaRDD<SAMRecord> reads,
            final SAMFileHeader header, final int numReducers, final HtsjdkReadsRddStorage.FormatWriteOption formatWriteOption,
            final HtsjdkReadsRddStorage.FileCardinalityWriteOption fileCardinalityWriteOption) throws IOException {

        final JavaRDD<SAMRecord> sortedReads = SparkUtils.sortReads(reads, header, numReducers);
        Broadcast<SAMFileHeader> headerBroadcast = ctx.broadcast(header);
        final JavaRDD<SAMRecord> sortedReadsWithHeader = sortedReads.map(read -> {
            read.setHeaderStrict(headerBroadcast.getValue());
            return read;
        });
        HtsjdkReadsRdd htsjdkReadsRdd = new HtsjdkReadsRdd(header, sortedReadsWithHeader);
        HtsjdkReadsRddStorage.makeDefault(ctx)
                .referenceSourcePath(referenceFile)
                .write(htsjdkReadsRdd, outputFile, formatWriteOption, fileCardinalityWriteOption);
    }

    private static void writeReadsADAM(
            final JavaSparkContext ctx, final String outputFile, final JavaRDD<SAMRecord> reads,
            final SAMFileHeader header) throws IOException {
        final SequenceDictionary seqDict = SequenceDictionary.fromSAMSequenceDictionary(header.getSequenceDictionary());
        final RecordGroupDictionary readGroups = RecordGroupDictionary.fromSAMHeader(header);
        final JavaPairRDD<Void, AlignmentRecord> rddAlignmentRecords =
                reads.map(read -> {
                    read.setHeaderStrict(header);
                    AlignmentRecord alignmentRecord = GATKReadToBDGAlignmentRecordConverter.convert(read, seqDict, readGroups);
                    read.setHeaderStrict(null); // Restore the header to its previous state so as not to surprise the caller
                    return alignmentRecord;
                }).mapToPair(alignmentRecord -> new Tuple2<>(null, alignmentRecord));
        // instantiating a Job is necessary here in order to set the Hadoop Configuration...
        final Job job = Job.getInstance(ctx.hadoopConfiguration());
        // ...here, which sets a config property that the AvroParquetOutputFormat needs when writing data. Specifically,
        // we are writing the Avro schema to the Configuration as a JSON string. The AvroParquetOutputFormat class knows
        // how to translate objects in the Avro data model to the Parquet primitives that get written.
        AvroParquetOutputFormat.setSchema(job, AlignmentRecord.getClassSchema());
        deleteHadoopFile(outputFile, ctx.hadoopConfiguration());
        rddAlignmentRecords.saveAsNewAPIHadoopFile(
                outputFile, Void.class, AlignmentRecord.class, AvroParquetOutputFormat.class, job.getConfiguration());
    }

    private static void deleteHadoopFile(String fileToObliterate, Configuration conf) throws IOException {
        final Path pathToDelete = new Path(fileToObliterate);
        pathToDelete.getFileSystem(conf).delete(pathToDelete, true);
    }

}
