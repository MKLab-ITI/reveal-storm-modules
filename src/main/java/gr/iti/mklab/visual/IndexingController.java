package gr.iti.mklab.visual;

import gr.iti.mklab.conf.Configuration;
import gr.iti.mklab.util.ImageUtils;
import gr.iti.mklab.visual.aggregation.AbstractFeatureAggregator;
import gr.iti.mklab.visual.aggregation.VladAggregatorMultipleVocabularies;
import gr.iti.mklab.visual.datastructures.AbstractSearchStructure;
import gr.iti.mklab.visual.datastructures.IVFPQ;
import gr.iti.mklab.visual.datastructures.PQ;
import gr.iti.mklab.visual.dimreduction.PCA;
import gr.iti.mklab.visual.extraction.AbstractFeatureExtractor;
import gr.iti.mklab.visual.extraction.SURFExtractor;
import gr.iti.mklab.visual.utilities.Answer;
import gr.iti.mklab.visual.vectorization.ImageVectorization;
import gr.iti.mklab.visual.vectorization.ImageVectorizationResult;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * A Controller responsible for feature extraction, indexing and visual similarity search
 *
 * @author kandreadou
 */
public class IndexingController {

    protected static int maxNumPixels = 768 * 512; // use 1024*768 for better/slower extraction
    protected static int targetLengthMax = 1024;
    protected static PCA pca;
    private static Map<String, AbstractSearchStructure> indices = new HashMap<String, AbstractSearchStructure>();
    private static IndexingController singletonInstance;

    private synchronized static IndexingController ensureInstance() throws IllegalStateException {
        if (singletonInstance == null) {
            singletonInstance = new IndexingController();
        }
        return singletonInstance;
    }

    private IndexingController() {
        try {
            System.out.println("IndexingController initialization");
            int[] numCentroids = {128, 128, 128, 128};
            int initialLength = numCentroids.length * numCentroids[0] * AbstractFeatureExtractor.SURFLength;

            String[] codebookFiles = {
                    Configuration.LEARNING_FOLDER + "surf_l2_128c_0.csv",
                    Configuration.LEARNING_FOLDER + "surf_l2_128c_1.csv",
                    Configuration.LEARNING_FOLDER + "surf_l2_128c_2.csv",
                    Configuration.LEARNING_FOLDER + "surf_l2_128c_3.csv"
            };

            String pcaFile = Configuration.LEARNING_FOLDER + "pca_surf_4x128_32768to1024.txt";

            SURFExtractor extractor = new SURFExtractor();
            ImageVectorization.setFeatureExtractor(extractor);
            double[][][] codebooks = AbstractFeatureAggregator.readQuantizers(codebookFiles, numCentroids,
                    AbstractFeatureExtractor.SURFLength);
            ImageVectorization.setVladAggregator(new VladAggregatorMultipleVocabularies(codebooks));
            if (targetLengthMax < initialLength) {
                System.out.println("targetLengthMax : " + targetLengthMax + " initialLengh " + initialLength);
                pca = new PCA(targetLengthMax, 1, initialLength, true);
                pca.loadPCAFromFile(pcaFile);
                ImageVectorization.setPcaProjector(pca);
            }
        } catch (Exception e) {
            throw new IllegalStateException("The Indexing Controller could not be properly initialized. ", e);
        }
    }

    public static void initialize() {
        ensureInstance();
    }

    /**
     * Creates an index with the given name and default size
     *
     * @param name
     * @throws IllegalStateException
     */
    public static void createIndex(String name) throws IllegalStateException {
        createIndex(name, Configuration.INDEX_SIZE);
    }

    /**
     * Creates an index with the given name and size
     *
     * @param name
     * @param maximumNumVectors
     * @throws IllegalStateException
     */
    public static void createIndex(String name, int maximumNumVectors) throws IllegalStateException {
        ensureInstance();
        String ivfpqIndexFolder = Configuration.INDEX_FOLDER + name;
        try {
            System.out.println("Loading index " + ivfpqIndexFolder);
            File jeLck = new File(ivfpqIndexFolder, "je.lck");
            if (jeLck.exists()) {
                jeLck.delete();
            } else {
                jeLck.getParentFile().getParentFile().mkdirs();
            }

            int m2 = 64;
            int k_c = 256;
            int numCoarseCentroids = 8192;
            String coarseQuantizerFile2 = Configuration.LEARNING_FOLDER + "qcoarse_1024d_8192k.csv";
            String productQuantizerFile2 = Configuration.LEARNING_FOLDER + "pq_1024_64x8_rp_ivf_8192k.csv";

            IVFPQ index = new IVFPQ(targetLengthMax, maximumNumVectors, false, ivfpqIndexFolder, m2, k_c, PQ.TransformationType.RandomPermutation, numCoarseCentroids, true, 0);
            index.loadCoarseQuantizer(coarseQuantizerFile2);
            index.loadProductQuantizer(productQuantizerFile2);
            int w = 64; // larger values will improve results/increase seach time
            index.setW(w); // how many (out of 8192) lists should be visited during search.
            if (indices != null) {
                indices.put(name, index);
            }
        } catch (Exception ex) {
            throw new IllegalStateException("The " + name + " index at " + ivfpqIndexFolder + " could not be created");
        }
    }

    /**
     * Indexes the image in the given path to the given collection
     *
     * @param imageFolder
     * @param imageFilename
     * @param collection
     * @return
     * @throws Exception
     */
    public static boolean indexImage(String imageFolder, String imageFilename, String collection) throws Exception {
        ensureInstance();
        AbstractSearchStructure index = indices.get(collection);
        if (index == null) {
            createIndex(collection);
            index = indices.get(collection);
        }
        ImageVectorization imvec = new ImageVectorization(imageFolder, imageFilename, targetLengthMax, maxNumPixels);
        ImageVectorizationResult imvr = imvec.call();
        double[] vector = imvr.getImageVector();
        if (vector == null || vector.length == 0)
            throw new IllegalArgumentException(imageFilename + "has an empty feature vector and it will not be indexed");
        return index.indexVector(imageFilename, vector);
    }

    /**
     * Indexes the image in the given url to the given collection
     *
     * @param url
     * @param collection
     * @return
     * @throws Exception
     */
    public static boolean indexImage(String url, String collection) {
        ensureInstance();
        AbstractSearchStructure index = indices.get(collection);
        if (index == null) {
            createIndex(collection);
            index = indices.get(collection);
        }
        BufferedImage img;
        try {
            img = ImageUtils.downloadImage(url);
        } catch (Exception e) {
            throw new IllegalArgumentException("Exeption on dowloading image " + url, e);
        }
        ImageVectorization imvec = new ImageVectorization(url, img, targetLengthMax, maxNumPixels);
        ImageVectorizationResult imvr = imvec.call();
        double[] vector = imvr.getImageVector();
        if (vector == null || vector.length == 0)
            throw new IllegalArgumentException("Image " + url + " has an empty feature vector and it will not be indexed");
        try {
            return index.indexVector(url, vector);
        } catch (Exception e) {
            throw new IllegalArgumentException("Image " + url + " could not be indexed", e);
        }
    }

    /**
     * Finds the n nearest neighbours to the given image in the given collection
     *
     * @param url
     * @param collection
     * @param neighbours
     * @return
     * @throws Exception
     */
    public static Answer findSimilar(String url, String collection, int neighbours) throws Exception {
        ensureInstance();
        AbstractSearchStructure index = indices.get(collection);
        if (index == null) {
            createIndex(collection);
            index = indices.get(collection);
        }
        BufferedImage img = ImageIO.read(new URL(url));
        ImageVectorization imvec = new ImageVectorization(url, img, targetLengthMax, maxNumPixels);
        ImageVectorizationResult imvr = imvec.call();
        double[] vector = imvr.getImageVector();
        if (vector == null || vector.length == 0)
            throw new IllegalArgumentException("Image " + url + "has an empty feature vector and it cannot be used for searching");
        return index.computeNearestNeighbors(neighbours, vector);
    }


    /**
     * Gets the statistics for the specified collection if it is loaded or throws exception if not
     *
     * @param collection
     * @return
     */
    public static int getStatistics(String collection) {
        ensureInstance();
        if (indices.containsKey(collection)) {
            AbstractSearchStructure index = indices.get(collection);
            int ivfpqIndexCount = index.getLoadCounter();
            System.out.println("Collection " + collection + " Load counter " + ivfpqIndexCount);
            return ivfpqIndexCount;
        } else
            throw new IllegalArgumentException("Collection " + collection + " not loaded");
    }
}
