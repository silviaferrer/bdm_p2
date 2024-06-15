# bdm_p2
Notes for the report:
- The neighborhood Sant Pere, Santa Caterina i la Ribera is not found on the income df and airqual df lookup because in the lookup tables it is written like "Sant Pere, Santa Caterina i la Ribera"

Steps:
    1. Reconciliate District and Neighbourhood names:
        Join the 3 tables airquality, idealista, income with the respective
        lookup tables and transform the District and Neighbourhood names (and
        ids) to the reconciled names.
        This will allow each user (analyst, ML engineer...) to join the tables
        without worrying about the names' formats.
    1.1.  Save dataframes in VM in the Formatted Zone
    2. Come up with KPIs:
        Start with the ones given in the presentation
    3. Create a star-schema:
        Create dimension and fact tables (with ids for the corresponding dimensions)
        from the Formatted Zone tables
    4. Calculate KPIs and visualize them graphically
        Store the calculated KPIs in the VM in the in the Exploitation Zone
        Create visualizations on PowerBI/Tableau
    5. Come up with a predictive analysis
        Start with the predictions given in the presentation
    6. Join the necessary tables from the Formatted Zone
    7. Train and test a model
    8. Save the model in the VM in the Exploitation Zone
    8. Ingest and process the Kafka stream
        Apply the same formatting used in the Formatted Zone
    9. Perform predictions using the model stored in the VM
    10. Visualize the predictions