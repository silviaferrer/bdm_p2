from pyspark.sql.functions import avg, desc, month, year, max
from pyspark.sql.types import DoubleType

from utils.utils import loadFromSpark, joinDf

class DescriptiveAnalysis():

    def __init__(self, spark, mongoLoader, logger):
        self.spark = spark
        self.mongoLoader = mongoLoader
        self.logger = logger

        self.collections = ['airqual', 'idealista', 'income']


    def main(self):
        dfs = loadFromSpark(self.spark, self.mongoLoader,
                            self.logger, self.collections)
        idealista_income = joinDf(self.logger, dfs['income'],
               dfs['idealista'], ['neighborhood'])
        df_idealista = dfs['idealista']
        dfs['idealista'] = df_idealista.withColumn(
            'year', year(df_idealista['date']))
        idealista_airqual = joinDf(self.logger, dfs['airqual'],
                                   dfs['idealista'], ['neighborhood', 'year'])

        self.logger.info('Calculating KPIs...')
        kpis = {}
        # Avg nº of listings per day
        listings_per_day = dfs['idealista'].groupBy('date').count()
        kpis['average_listings_per_day'] = listings_per_day.select(avg('count')).collect()[0][0]
        self.logger.info(f'Average number of new listings per day: {kpis['average_listings_per_day']}')

        # Top-seller neighborhoods in the last month
        # Get the last month and year
        last_month_year = dfs['idealista'].select(max('date')).collect()[0][0]
        last_month = last_month_year.month
        last_year = last_month_year.year

        # Filter for the last month
        last_month_df = dfs['idealista'].filter((month(dfs['idealista']['date']) == last_month) & (
            year(dfs['idealista']['date']) == last_year))

        # Calculate top-seller neighborhoods
        top_seller_neighborhoods = last_month_df.groupBy(
            'neighborhood').count().orderBy(desc('count'))

        self.logger.info('Top-seller neighborhoods in the last month:' + 
                         top_seller_neighborhoods._show_string(5))


        # Correlation of sale price and family income per neighborhood
        kpis['correlation_price_RFD'] = idealista_income.stat.corr('price', 'RFD')
        self.logger.info(f'Correlation between price and RFD: {kpis['correlation_price_RFD']}')


        # Correlation of sale price and air contaminants
        # Convert 'Codi_Contaminant' to numeric
        idealista_airqual = idealista_airqual.withColumn(
            'Codi_Contaminant', idealista_airqual['Codi_Contaminant'].cast(DoubleType()))

        kpis['correlation_price_contaminants'] = idealista_airqual.stat\
            .corr('price', 'Codi_Contaminant')
        self.logger.info(f'Correlation between price and air contaminants:\
                          {kpis['correlation_price_contaminants']}')

        self.logger.info('KPIs calculated!')

        self.logger.info('Saving KPIs to MongoDB...')
        # Convert kpis to DataFrame
        kpis_df = self.spark.createDataFrame(
            [(k, v) for k, v in kpis.items()], ["KPI", "Value"])
        self.mongoLoader.write_to_collection('ExploitationZone', kpis_df)
        self.mongoLoader.write_to_collection('ExploitationZone', top_seller_neighborhoods)

        self.logger.info('KPIs saved!')

        return None