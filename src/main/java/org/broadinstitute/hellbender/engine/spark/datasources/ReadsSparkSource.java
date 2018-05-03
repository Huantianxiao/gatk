package org.broadinstitute.hellbender.engine.spark.datasources;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.tom_e_white.squark.HtsjdkReadsRdd;
import com.tom_e_white.squark.HtsjdkReadsRddStorage;
import com.tom_e_white.squark.HtsjdkReadsTraversalParameters;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.ValidationStringency;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.broadinstitute.hellbender.engine.ReadsDataSource;
import org.broadinstitute.hellbender.engine.TraversalParameters;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.broadinstitute.hellbender.utils.read.BDGAlignmentRecordToGATKReadAdapter;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadConstants;
import org.broadinstitute.hellbender.utils.read.SAMRecordToGATKReadAdapter;
import org.broadinstitute.hellbender.utils.spark.SparkUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/** Loads the reads from disk either serially (using samReaderFactory) or in parallel using Hadoop-BAM.
 * The parallel code is a modified version of the example writing code from Hadoop-BAM.
 */
public final class ReadsSparkSource implements Serializable {
    private static final long serialVersionUID = 1L;

    private transient final JavaSparkContext ctx;
    private ValidationStringency validationStringency = ReadConstants.DEFAULT_READ_VALIDATION_STRINGENCY;

    private static final Logger logger = LogManager.getLogger(ReadsSparkSource.class);

    public ReadsSparkSource(final JavaSparkContext ctx) { this.ctx = ctx; }

    public ReadsSparkSource(final JavaSparkContext ctx, final ValidationStringency validationStringency)
    {
        this.ctx = ctx;
        this.validationStringency = validationStringency;
    }


    /**
     * Loads Reads using Hadoop-BAM. For local files, readFileName must have the fully-qualified path,
     * i.e., file:///path/to/bam.bam.
     * @param readFileName file to load
     * @param referencePath Reference path or null if not available. Reference is required for CRAM files.
     * @param traversalParameters parameters controlling which reads to include. If <code>null</code> then all the reads (both mapped and unmapped) will be returned.
     * @return RDD of (SAMRecord-backed) GATKReads from the file.
     */
    public JavaRDD<GATKRead> getParallelReads(final String readFileName, final String referencePath, final TraversalParameters traversalParameters) {
        return getParallelReads(readFileName, referencePath, traversalParameters, 0);
    }

    /**
     * Loads Reads using Hadoop-BAM. For local files, bam must have the fully-qualified path,
     * i.e., file:///path/to/bam.bam.
     * @param readFileName file to load
     * @param referencePath Reference path or null if not available. Reference is required for CRAM files.
     * @param traversalParameters parameters controlling which reads to include. If <code>null</code> then all the reads (both mapped and unmapped) will be returned.
     * @param splitSize maximum bytes of bam file to read into a single partition, increasing this will result in fewer partitions. A value of zero means
     *                  use the default split size (determined by the Hadoop input format, typically the size of one HDFS block).
     * @return RDD of (SAMRecord-backed) GATKReads from the file.
     */
    public JavaRDD<GATKRead> getParallelReads(final String readFileName, final String referencePath, final TraversalParameters traversalParameters, final long splitSize) {
        try {
            checkCramReference(ctx, readFileName, referencePath);
            HtsjdkReadsTraversalParameters<SimpleInterval> tp = traversalParameters == null ? null :
                    new HtsjdkReadsTraversalParameters<>(traversalParameters.getIntervalsForTraversal(), traversalParameters.traverseUnmappedReads());
            HtsjdkReadsRdd htsjdkReadsRdd = HtsjdkReadsRddStorage.makeDefault(ctx)
                    .splitSize((int) splitSize)
                    .validationStringency(validationStringency)
                    .referenceSourcePath(referencePath)
                    .read(readFileName, tp);
            JavaRDD<GATKRead> reads = htsjdkReadsRdd.getReads()
                    .map(read -> (GATKRead) SAMRecordToGATKReadAdapter.headerlessReadAdapter(read))
                    .filter(Objects::nonNull);
            return putPairsInSamePartition(htsjdkReadsRdd.getHeader(), reads);
        } catch (IOException | IllegalArgumentException e) {
            throw new UserException("Failed to load reads from " + readFileName + "\n Caused by:" + e.getMessage(), e);
        }
    }

    /**
     * Loads Reads using Hadoop-BAM. For local files, readFileName must have the fully-qualified path,
     * i.e., file:///path/to/bam.bam.
     * @param readFileName file to load
     * @param referencePath Reference path or null if not available. Reference is required for CRAM files.
     * @return RDD of (SAMRecord-backed) GATKReads from the file.
     */
    public JavaRDD<GATKRead> getParallelReads(final String readFileName, final String referencePath) {
        return getParallelReads(readFileName, referencePath, 0);
    }

    /**
     * Loads Reads using Hadoop-BAM. For local files, readFileName must have the fully-qualified path,
     * i.e., file:///path/to/bam.bam.
     * @param readFileName file to load
     * @param referencePath Reference path or null if not available. Reference is required for CRAM files.
     * @param splitSize maximum bytes of bam file to read into a single partition, increasing this will result in fewer partitions. A value of zero means
     *                  use the default split size (determined by the Hadoop input format, typically the size of one HDFS block).
     * @return RDD of (SAMRecord-backed) GATKReads from the file.
     */
    public JavaRDD<GATKRead> getParallelReads(final String readFileName, final String referencePath, int splitSize) {
        return getParallelReads(readFileName, referencePath, null /* all reads */, splitSize);
    }

    /**
     * Loads ADAM reads stored as Parquet.
     * @param inputPath path to the Parquet data
     * @return RDD of (ADAM-backed) GATKReads from the file.
     */
    public JavaRDD<GATKRead> getADAMReads(final String inputPath, final TraversalParameters traversalParameters, final SAMFileHeader header) throws IOException {
        Job job = Job.getInstance(ctx.hadoopConfiguration());
        AvroParquetInputFormat.setAvroReadSchema(job, AlignmentRecord.getClassSchema());
        Broadcast<SAMFileHeader> bHeader;
        if (header == null) {
            bHeader= ctx.broadcast(null);
        } else {
            bHeader = ctx.broadcast(header);
        }
        @SuppressWarnings("unchecked")
        JavaRDD<AlignmentRecord> recordsRdd = ctx.newAPIHadoopFile(
                inputPath, AvroParquetInputFormat.class, Void.class, AlignmentRecord.class, job.getConfiguration())
                .values();
        JavaRDD<GATKRead> readsRdd = recordsRdd.map(record -> new BDGAlignmentRecordToGATKReadAdapter(record, bHeader.getValue()));
        JavaRDD<GATKRead> filteredRdd = readsRdd.filter(record -> samRecordOverlaps(record.convertToSAMRecord(header), traversalParameters));
        return putPairsInSamePartition(header, filteredRdd);
    }

    /**
     * Loads the header using Hadoop-BAM.
     * @param filePath path to the bam.
     * @param referencePath Reference path or null if not available. Reference is required for CRAM files.
     * @return the header for the bam.
     */
    public SAMFileHeader getHeader(final String filePath, final String referencePath) {
        // GCS case
        if (BucketUtils.isCloudStorageUrl(filePath)) {
            try (ReadsDataSource readsDataSource = new ReadsDataSource(IOUtils.getPath(filePath))) {
                return readsDataSource.getHeader();
            }
        }

        // local file or HDFs case
        try {
            checkCramReference(ctx, filePath, referencePath);
            return HtsjdkReadsRddStorage.makeDefault(ctx)
                    .validationStringency(validationStringency)
                    .referenceSourcePath(referencePath)
                    .read(filePath)
                    .getHeader();
        } catch (IOException | IllegalArgumentException e) {
            throw new UserException("Failed to read bam header from " + filePath + "\n Caused by:" + e.getMessage(), e);
        }
    }

    /**
     * Ensure reads in a pair fall in the same partition (input split), if the reads are queryname-sorted,
     * so they are processed together. No shuffle is needed.
     */
    JavaRDD<GATKRead> putPairsInSamePartition(final SAMFileHeader header, final JavaRDD<GATKRead> reads) {
        if (!header.getSortOrder().equals(SAMFileHeader.SortOrder.queryname)) {
            return reads;
        }
        int numPartitions = reads.getNumPartitions();
        final String firstGroupInBam = reads.first().getName();
        // Find the first group in each partition
        List<List<GATKRead>> firstReadNamesInEachPartition = reads
                .mapPartitions(it -> { PeekingIterator<GATKRead> current = Iterators.peekingIterator(it);
                                List<GATKRead> firstGroup = new ArrayList<>(2);
                                firstGroup.add(current.next());
                                String name = firstGroup.get(0).getName();
                                while (current.hasNext() && current.peek().getName().equals(name)) {
                                    firstGroup.add(current.next());
                                }
                                return Iterators.singletonIterator(firstGroup);
                                })
                .collect();

        // Checking for pathological cases (read name groups that span more than 2 partitions)
        String groupName = null;
        for (List<GATKRead> group : firstReadNamesInEachPartition) {
            if (group!=null && !group.isEmpty()) {
                // If a read spans multiple partitions we expect its name to show up multiple times and we don't expect this to work properly
                if (groupName != null && group.get(0).getName().equals(groupName)) {
                    throw new GATKException(String.format("The read name '%s' appeared across multiple partitions this could indicate there was a problem " +
                            "with the sorting or that the rdd has too many partitions, check that the file is queryname sorted and consider decreasing the number of partitions", groupName));
                }
                groupName =  group.get(0).getName();
            }
        }

        // Shift left, so that each partition will be joined with the first read group from the _next_ partition
        List<List<GATKRead>> firstReadInNextPartition = new ArrayList<>(firstReadNamesInEachPartition.subList(1, numPartitions));
        firstReadInNextPartition.add(null); // the last partition does not have any reads to add to it

        // Join the reads with the first read from the _next_ partition, then filter out the first and/or last read if not in a pair
        return reads.zipPartitions(ctx.parallelize(firstReadInNextPartition, numPartitions),
                (FlatMapFunction2<Iterator<GATKRead>, Iterator<List<GATKRead>>, GATKRead>) (it1, it2) -> {
            PeekingIterator<GATKRead> current = Iterators.peekingIterator(it1);
            String firstName = current.peek().getName();
            // Make sure we don't remove reads from the first partition
            if (!firstGroupInBam.equals(firstName)) {
                // skip the first read name group in the _current_ partition if it is the second in a pair since it will be handled in the previous partition
                while (current.hasNext() && current.peek() != null && current.peek().getName().equals(firstName)) {
                    current.next();
                }
            }
            // append the first reads in the _next_ partition to the _current_ partition
            PeekingIterator<List<GATKRead>> next = Iterators.peekingIterator(it2);
            if (next.hasNext() && next.peek() != null) {
                return Iterators.concat(current, next.peek().iterator());
            }
            return current;
        });
    }

    static void checkCramReference(final JavaSparkContext ctx, final String filePath, final String referencePath) {
        if (IOUtils.isCramFileName(filePath)) {
            if (referencePath == null) {
                throw new UserException.MissingReference("A reference is required for CRAM input");
            } else if (ReferenceTwoBitSource.isTwoBit(referencePath)) { // htsjdk can't handle 2bit reference files
                throw new UserException("A 2bit file cannot be used as a CRAM file reference");
            } else {
                final Path refPath = new Path(referencePath);
                if (!SparkUtils.pathExists(ctx, refPath)) {
                    throw new UserException.MissingReference("The specified fasta file (" + referencePath + ") does not exist.");
                }
            }
        }
    }

    /**
     * Tests if a given SAMRecord overlaps any interval in a collection. This is only used as a fallback option for
     * formats that don't support query-by-interval natively at the Hadoop-BAM layer.
     */
    //TODO: use IntervalsSkipList, see https://github.com/broadinstitute/gatk/issues/1531
    private static boolean samRecordOverlaps(final SAMRecord record, final TraversalParameters traversalParameters ) {
        if (traversalParameters == null) {
            return true;
        }
        if (traversalParameters.traverseUnmappedReads() && record.getReadUnmappedFlag() && record.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START) {
            return true; // include record if unmapped records should be traversed and record is unmapped
        }
        List<SimpleInterval> intervals = traversalParameters.getIntervalsForTraversal();
        if (intervals == null || intervals.isEmpty()) {
            return false; // no intervals means 'no mapped reads'
        }
        for (SimpleInterval interval : intervals) {
            if (record.getReadUnmappedFlag() && record.getAlignmentStart() != SAMRecord.NO_ALIGNMENT_START) {
                // This follows the behavior of htsjdk's SamReader which states that "an unmapped read will be returned
                // by this call if it has a coordinate for the purpose of sorting that is in the query region".
                int start = record.getAlignmentStart();
                return interval.getStart() <= start && interval.getEnd() >= start;
            } else  if (interval.overlaps(record)) {
                return true;
            }
        }
        return false;
    }
}
