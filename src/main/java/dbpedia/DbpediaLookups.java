package dbpedia;

public class DbpediaLookups {
    private final ImagesLookup imagesLookup;
    private final LabelsLookup labelsLookup;
    private final InterlinksLookup interlinksLookup;
    private final ArticleCategoryLookup articleCategoryLookup;

    public DbpediaLookups(ImagesLookup imagesLookup,
                          LabelsLookup labelsLookup,
                          InterlinksLookup interlinksLookup,
                          ArticleCategoryLookup articleCategoryLookup) {
        this.imagesLookup = imagesLookup;
        this.labelsLookup = labelsLookup;
        this.interlinksLookup = interlinksLookup;
        this.articleCategoryLookup = articleCategoryLookup;
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

    public ArticleCategoryLookup getArticleCategoryLookup() {
        return articleCategoryLookup;
    }
}
