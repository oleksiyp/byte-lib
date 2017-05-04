package dbpedia;

public class DbpediaLookups {
    private final ImagesLookup imagesLookup;
    private final LabelsLookup labelsLookup;
    private final InterlinksLookup interlinksLookup;

    public DbpediaLookups(ImagesLookup imagesLookup,
                          LabelsLookup labelsLookup,
                          InterlinksLookup interlinksLookup) {
        this.imagesLookup = imagesLookup;
        this.labelsLookup = labelsLookup;
        this.interlinksLookup = interlinksLookup;
    }

    public ImagesLookup getImagesLookup() {
        return imagesLookup;
    }

    public LabelsLookup getLabelsLookup() {
        return labelsLookup;
    }

    public InterlinksLookup getInterlinksLookup() {
        return interlinksLookup;
    }
}
