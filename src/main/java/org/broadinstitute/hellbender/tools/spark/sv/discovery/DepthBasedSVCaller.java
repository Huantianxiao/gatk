package org.broadinstitute.hellbender.tools.spark.sv.discovery;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.OverlapDetector;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.tools.copynumber.formats.collections.CalledCopyRatioSegmentCollection;
import org.broadinstitute.hellbender.tools.copynumber.formats.collections.CopyRatioCollection;
import org.broadinstitute.hellbender.tools.copynumber.formats.records.CalledCopyRatioSegment;
import org.broadinstitute.hellbender.tools.copynumber.formats.records.CopyRatio;
import org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.inference.IntervalUtils;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.inference.LargeSimpleSV;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.inference.LargeTandemDuplicationFactory;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.inference.SimpleSVFactory;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.EvidenceTargetLink;
import org.broadinstitute.hellbender.tools.spark.sv.utils.GATKSVVCFConstants;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DepthBasedSVCaller {

    private static final Logger logger = LogManager.getLogger(DepthBasedSVCaller.class);

    private final OverlapDetector<CopyRatio> copyRatioOverlapDetector;
    private final SVIntervalTree<EvidenceTargetLink> intrachromosomalLinkTree;
    private final SVIntervalTree<EvidenceTargetLink> interchromosomalLinkTree;
    private final SVIntervalTree<GATKRead> contigTree;
    private final OverlapDetector<CalledCopyRatioSegment> copyRatioSegmentOverlapDetector;
    private final SimpleSVFactory tandemDuplicationFactory;
    private final SAMSequenceDictionary dictionary;
    private final List<IntrachromosomalBreakpointPair> pairedBreakpoints;
    private final StructuralVariationDiscoveryArgumentCollection.SimpleVariantDiscoveryArgumentCollection arguments;

    public DepthBasedSVCaller(final List<VariantContext> breakpoints,
                              final List<GATKRead> assembledContigs,
                              final List<EvidenceTargetLink> evidenceTargetLinks,
                              final CopyRatioCollection copyRatios,
                              final CalledCopyRatioSegmentCollection copyRatioSegments,
                              final SAMSequenceDictionary dictionary,
                              final StructuralVariationDiscoveryArgumentCollection.SimpleVariantDiscoveryArgumentCollection arguments) {
        Utils.nonNull(breakpoints, "Breakpoint list cannot be null");
        Utils.nonNull(assembledContigs, "Contig list cannot be null");
        Utils.nonNull(evidenceTargetLinks, "Evidence target link list cannot be null");
        Utils.nonNull(copyRatios, "Copy ratio collection cannot be null");
        Utils.nonNull(copyRatioSegments, "Copy ratio segments collection cannot be null");
        Utils.nonNull(dictionary, "Dictionary cannot be null");
        Utils.nonNull(arguments, "Parameter arguments collection cannot be null");

        this.dictionary = dictionary;
        this.arguments = arguments;

        logger.info("Building interval trees...");

        final List<EvidenceTargetLink> intrachromosomalEvidenceLinkTargets = getIntrachromosomalLinks(evidenceTargetLinks);
        final List<EvidenceTargetLink> interchromosomalEvidenceLinkTargets = getInterchromosomalLinks(evidenceTargetLinks);
        intrachromosomalLinkTree = buildEvidenceIntervalTree(intrachromosomalEvidenceLinkTargets, 0, false);
        interchromosomalLinkTree = buildEvidenceIntervalTree(interchromosomalEvidenceLinkTargets, 0, true);

        pairedBreakpoints = getIntrachromosomalBreakpointPairs(breakpoints);
        contigTree = buildReadIntervalTree(assembledContigs);
        copyRatioOverlapDetector = getCopyRatioOverlapDetector(copyRatios, pairedBreakpoints,
                Arrays.asList(intrachromosomalLinkTree, interchromosomalLinkTree), arguments.HMM_PADDING, dictionary);
        copyRatioSegmentOverlapDetector = copyRatioSegments.getOverlapDetector();

        logger.info("Initializing event factories...");
        tandemDuplicationFactory = new LargeTandemDuplicationFactory(intrachromosomalLinkTree, interchromosomalLinkTree,
                contigTree, arguments, copyRatioSegmentOverlapDetector, copyRatioOverlapDetector, dictionary);

    }

    private static OverlapDetector<CopyRatio> getCopyRatioOverlapDetector(final CopyRatioCollection copyRatios,
                                                           final List<IntrachromosomalBreakpointPair> breakpoints,
                                                           final List<SVIntervalTree<?>> evidenceTrees,
                                                           final int padding,
                                                           final SAMSequenceDictionary dictionary) {
        final SVIntervalTree<IntrachromosomalBreakpointPair> breakpointTree = buildBreakpointTree(breakpoints);
        final List<CopyRatio> countsList = copyRatios.getRecords().stream()
                .filter(ratio -> {
                    final SVInterval interval = IntervalUtils.convertInterval(ratio.getInterval(), dictionary);
                    final SVInterval paddedInterval = IntervalUtils.getPaddedInterval(interval, padding, dictionary);
                    return breakpointTree.hasOverlapper(paddedInterval) || evidenceTrees.stream().anyMatch(tree -> tree.hasOverlapper(paddedInterval));
                }).collect(Collectors.toList());
        return new CopyRatioCollection(copyRatios.getMetadata(), countsList).getOverlapDetector();
    }

    private static SVIntervalTree<IntrachromosomalBreakpointPair> buildBreakpointTree(final List<IntrachromosomalBreakpointPair> breakpoints) {
        final SVIntervalTree<IntrachromosomalBreakpointPair> tree = new SVIntervalTree<>();
        for (final IntrachromosomalBreakpointPair breakpointPair : breakpoints) {
            tree.put(breakpointPair.getInterval(), breakpointPair);
        }
        return tree;
    }


    /**
     * Returns only links that are on the same chromosome
     */
    private static List<EvidenceTargetLink> getIntrachromosomalLinks(final List<EvidenceTargetLink> links) {
        return links.stream().filter(link -> link.getPairedStrandedIntervals().getLeft().getInterval().getContig() ==
                link.getPairedStrandedIntervals().getRight().getInterval().getContig()).collect(Collectors.toList());
    }

    /**
     * Returns only links that are on different chromosomes
     */
    private static List<EvidenceTargetLink> getInterchromosomalLinks(final List<EvidenceTargetLink> links) {
        return links.stream().filter(link -> link.getPairedStrandedIntervals().getLeft().getInterval().getContig() !=
                link.getPairedStrandedIntervals().getRight().getInterval().getContig()).collect(Collectors.toList());
    }

    /**
     * Returns paired breakpoints on the same chromosome
     */
    private List<IntrachromosomalBreakpointPair> getIntrachromosomalBreakpointPairs(final List<VariantContext> breakpoints) {
        final Map<String, VariantContext> unpairedVariants = new HashMap<>();
        final List<IntrachromosomalBreakpointPair> pairedBreakpoints = new ArrayList<>(breakpoints.size() / 2);
        final Iterator<VariantContext> breakpointIter = breakpoints.iterator();
        while (breakpointIter.hasNext()) {
            final VariantContext vc1 = breakpointIter.next();
            if (!vc1.hasAttribute(GATKSVVCFConstants.BND_MATEID_STR)) continue;
            final String mate = vc1.getAttributeAsString(GATKSVVCFConstants.BND_MATEID_STR, "");
            if (unpairedVariants.containsKey(mate)) {
                final VariantContext vc2 = unpairedVariants.remove(mate);
                if (isBreakpointPair(vc1, vc2)) {
                    final int contig = dictionary.getSequenceIndex(vc1.getContig());
                    final VariantContext first;
                    final VariantContext second;
                    if (vc1.getStart() < vc2.getStart()) {
                        first = vc1;
                        second = vc2;
                    } else {
                        first = vc2;
                        second = vc1;
                    }
                    final int start = first.getStart();
                    final int end = second.getStart();
                    final List<String> firstContigs = first.getAttributeAsStringList(GATKSVVCFConstants.CONTIG_NAMES, "");
                    final List<String> secondContigs = second.getAttributeAsStringList(GATKSVVCFConstants.CONTIG_NAMES, "");
                    pairedBreakpoints.add(new IntrachromosomalBreakpointPair(contig, start, end, firstContigs, secondContigs));
                } else {
                    throw new IllegalStateException("Variant mate attributes did not match: " + vc1 + "\t" + vc2);
                }
            } else {
                unpairedVariants.put(vc1.getID(), vc1);
            }
        }
        if (!unpairedVariants.isEmpty()) {
            logger.warn("There were " + unpairedVariants.size() + " unpaired breakpoint variants with a " + GATKSVVCFConstants.BND_MATEID_STR + " attribute.");
        }
        return pairedBreakpoints;
    }

    /**
     * Returns true if the two BNDs in vc1 and vc2 are a valid breakpoint pair, as indicated by their MATEID attributes
     */
    private static boolean isBreakpointPair(final VariantContext vc1, final VariantContext vc2) {
        return vc1.hasAttribute(GATKSVVCFConstants.BND_MATEID_STR) &&
                vc1.getAttributeAsString(GATKSVVCFConstants.BND_MATEID_STR, "").equals(vc2.getID()) &&
                vc2.getAttributeAsString(GATKSVVCFConstants.BND_MATEID_STR, "").equals(vc1.getID());
    }

    /**
     * Builds an interval tree containing the only the aligned reads
     */
    private SVIntervalTree<GATKRead> buildReadIntervalTree(final List<GATKRead> reads) {
        final SVIntervalTree<GATKRead> tree = new SVIntervalTree<>();
        for (final GATKRead read : reads) {
            if (read.isUnmapped()) continue;
            final int start = read.getStart();
            final int end = read.getEnd();
            final int contig = dictionary.getSequenceIndex(read.getContig());
            tree.put(new SVInterval(contig, start, end), read);
        }
        return tree;
    }

    /**
     * Builds an interval tree of target evidence links, whose intervals may be padded. Tree may be built using
     * single intervals for each link (start of left to end of right) or two intervals (one for left and one for right)
     * @param links Target evidence links used to build the tree
     * @param padding Padding applied to intervals
     * @param separateLeftRightIntervals If true, inserts intervals for left and right intervals
     * @return The interval tree
     */
    private SVIntervalTree<EvidenceTargetLink> buildEvidenceIntervalTree(final List<EvidenceTargetLink> links, final int padding, final boolean separateLeftRightIntervals) {
        final SVIntervalTree<EvidenceTargetLink> linkTree = new SVIntervalTree<>();
        for (final EvidenceTargetLink link : links) {
            final SVInterval linkLeftInterval = link.getPairedStrandedIntervals().getLeft().getInterval();
            final SVInterval linkRightInterval = link.getPairedStrandedIntervals().getRight().getInterval();
            if (separateLeftRightIntervals) {
                final SVInterval eventIntervalLeft = new SVInterval(linkLeftInterval.getContig(), linkLeftInterval.getStart(), linkLeftInterval.getEnd());
                final SVInterval paddedEventIntervalLeft = IntervalUtils.getPaddedInterval(eventIntervalLeft, padding, dictionary);
                linkTree.put(paddedEventIntervalLeft, link);
                final SVInterval eventIntervalRight = new SVInterval(linkRightInterval.getContig(), linkRightInterval.getStart(), linkRightInterval.getEnd());
                final SVInterval paddedEventIntervalRight = IntervalUtils.getPaddedInterval(eventIntervalRight, padding, dictionary);
                linkTree.put(paddedEventIntervalRight, link);
            } else if (linkLeftInterval.getContig() == linkRightInterval.getContig()) {
                final SVInterval eventInterval = new SVInterval(linkLeftInterval.getContig(), linkLeftInterval.getStart(), linkRightInterval.getEnd());
                final SVInterval paddedEventInterval = IntervalUtils.getPaddedInterval(eventInterval, padding, dictionary);
                linkTree.put(paddedEventInterval, link);
            }
        }
        return linkTree;
    }


    /**
     * Gets the event with the highest score on the interval defned by leftInterval and rightInterval. For evidence
     * target links to count as evidence, their paired intervals must overlap leftInterval and rightInterval.
     * @param leftInterval The left interval
     * @param rightInterval The right interval
     * @param callInterval Calls will be made using this interval
     * @param disallowedIntervals No calls will be made if they sufficiently overlap intervals in this tree
     * @param breakpoints Breakpoints associated with the interval (may be null)
     * @return Optional containing the called event, if any
     */
    private Optional<LargeSimpleSV> getHighestScoringEventOnInterval(final SVInterval leftInterval, final SVInterval rightInterval, final SVInterval callInterval, final SVIntervalTree<LargeSimpleSV> disallowedIntervals, final IntrachromosomalBreakpointPair breakpoints, final int evidencePadding) {
        if (IntervalUtils.hasReciprocalOverlapInTree(callInterval, disallowedIntervals, arguments.MAX_CANDIDATE_EVENT_RECIPROCAL_OVERLAP)) return Optional.empty();
        final Stream<LargeSimpleSV> candidateEvents = getEventsOnInterval(leftInterval, rightInterval, callInterval, breakpoints, evidencePadding).stream();
        return candidateEvents.max(Comparator.comparingDouble(event -> event.getScore(arguments.COUNTEREVIDENCE_PSEUDOCOUNT)));
    }

    /**
     * Returns all events. Searches by iterating over the breakpoint pairs and then the evidence target links.
     */
    public List<LargeSimpleSV> callEvents() {

        //In order to prevent duplicate calls, keep calls in a tree and filter subsequent intervals with sufficient overlap
        final SVIntervalTree<LargeSimpleSV> calledEventTree = new SVIntervalTree<>();

        //Search breakpoint pairs
        for (final IntrachromosomalBreakpointPair breakpoints : pairedBreakpoints) {
            final SVInterval leftBreakpointInterval = new SVInterval(breakpoints.getContig(), breakpoints.getInterval().getStart(), breakpoints.getInterval().getStart());
            final SVInterval rightBreakpointInterval = new SVInterval(breakpoints.getContig(), breakpoints.getInterval().getEnd(), breakpoints.getInterval().getEnd());
            if (breakpoints.getInterval().getStart() == 1728544) {
                int x = 0;
            }
            final Optional<LargeSimpleSV> event = getHighestScoringEventOnInterval(leftBreakpointInterval, rightBreakpointInterval, breakpoints.getInterval(), calledEventTree, breakpoints, arguments.BREAKPOINT_INTERVAL_PADDING);
            if (event.isPresent()) {
                calledEventTree.put(event.get().getInterval(), event.get());
            }
        }

        //Search links
        final Iterator<SVIntervalTree.Entry<EvidenceTargetLink>> linkIter = intrachromosomalLinkTree.iterator();
        while (linkIter.hasNext()) {
            final EvidenceTargetLink link = linkIter.next().getValue();
            final SVInterval leftInterval = link.getPairedStrandedIntervals().getLeft().getInterval();
            final SVInterval rightInterval = link.getPairedStrandedIntervals().getRight().getInterval();
            final int callStart = (leftInterval.getStart() + leftInterval.getEnd()) / 2;
            final int callEnd = (rightInterval.getStart() + rightInterval.getEnd()) / 2;
            final SVInterval callInterval = new SVInterval(leftInterval.getContig(), callStart, callEnd);
            if (leftInterval.getStart() == 1728544) {
                int x = 0;
            }
            final Optional<LargeSimpleSV> event = getHighestScoringEventOnInterval(leftInterval, rightInterval, callInterval, calledEventTree, null, arguments.EVIDENCE_LINK_INTERVAL_PADDING);
            if (event.isPresent()) {
                calledEventTree.put(event.get().getInterval(), event.get());
            }
        }

        return Utils.stream(calledEventTree.iterator()).map(SVIntervalTree.Entry::getValue).collect(Collectors.toList());
    }

    /**
     * Gets list of valid events on the interval. Evidence target link stranded intervals must overlap leftInterval and rightInterval.
     * @param leftInterval The left interval
     * @param rightInterval The right interval
     * @param callInterval The interval to use for the call
     * @param breakpoints Associated breakpoints (may be null)
     * @return List of called events
     */
    private List<LargeSimpleSV> getEventsOnInterval(final SVInterval leftInterval,
                                                    final SVInterval rightInterval,
                                                    final SVInterval callInterval,
                                                    final IntrachromosomalBreakpointPair breakpoints,
                                                    final int evidencePadding) {

        if (leftInterval.getContig() != rightInterval.getContig()) return Collections.emptyList();
        if (callInterval.getLength() < arguments.MIN_SV_SIZE)
            return Collections.emptyList();

        final List<LargeSimpleSV> events = new ArrayList<>();

        final LargeSimpleSV tandemDuplication = tandemDuplicationFactory.create(leftInterval, rightInterval, callInterval, breakpoints, evidencePadding);
        if (tandemDuplication != null) events.add(tandemDuplication);

        return events;
    }

}
