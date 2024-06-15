# bdm_p2

Steps:
    1. Reconciliate District and Neighborhood names:
        Join the 3 tables airquality, idealista, income with the respective
        lookup tables and transform the District and Neighborhood names (and
        ids) to the reconciled names.
        This will allow each user (analyst, ML engineer...) to join the tables
        without worrying about the names' formats.
    2. Come up with KPIs:
        Start with the ones given in the presentation
    3. Create a star-schema:
        Create dimension and fact tables (with ids for the corresponding dimensions)
        from the Formatted Zone tables
    4. Calculate KPIs and visualize them graphically
        Store the calculated KPIs in the Exploitation Zone
        Create visualizations on PowerBI/Tableau
    5. Come up with a predictive analysis
        Start with the predictions given in the presentation
    6. Join the necessary tables from the Formatted Zone
    7. Train and test a model
    8. Save the model in the Exploitation Zone
    8. Ingest and process the Kafka stream
        Apply the same formatting used in the Formatted Zone
    9. Perform predictions using the stored model
    10. Visualize the predictions