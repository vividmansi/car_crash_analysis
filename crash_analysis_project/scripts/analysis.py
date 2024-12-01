from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame


class CrashAnalysis:
    """
    A class to perform various crash data analyses using PySpark DataFrames.
    """

    def __init__(self, df_person: DataFrame, df_charges: DataFrame, df_endorsements: DataFrame, 
                 df_restrict: DataFrame, df_damages: DataFrame, df_unit: DataFrame):
        """
        Initialize the CrashAnalysis class with necessary DataFrames.

        Args:
            df_person (DataFrame): Primary person DataFrame.
            df_charges (DataFrame): Charges DataFrame.
            df_endorsements (DataFrame): Endorsements DataFrame.
            df_restrict (DataFrame): Restrictions DataFrame.
            df_damages (DataFrame): Damages DataFrame.
            df_unit (DataFrame): Unit DataFrame.
        """
        self.df_person = df_person
        self.df_charges = df_charges
        self.df_endorsements = df_endorsements
        self.df_restrict = df_restrict
        self.df_damages = df_damages
        self.df_unit = df_unit

    def crash_analysis_1(self) -> DataFrame:
        """ 
        Find crashes where the number of males killed is greater than 2.

        Returns:
            DataFrame: Crashes with the count of males killed >= 2.
        """
        filtered_df = self.df_person.filter(
            (F.col("PRSN_GNDR_ID") == "MALE") & (F.col("PRSN_INJRY_SEV_ID") == "KILLED")
        )
        crash_count_df = filtered_df.groupBy("CRASH_ID").count()
        return crash_count_df.filter(F.col("count") > 2)

    def crash_analysis_2(self) -> int:
        """
        Count the number of crashes involving two-wheelers.

        Returns:
            int: Count of two-wheeler crashes.
        """
        return self.df_unit.filter(
            (F.col("VEH_BODY_STYL_ID") == "MOTORCYCLE") | (F.col("VEH_BODY_STYL_ID") == "POLICE MOTORCYCLE")
        ).count()

    def crash_analysis_3(self) -> DataFrame:
        """
        Get the top 5 vehicle makes where the driver was killed and airbags did not deploy.

        Returns:
            DataFrame: Top 5 vehicle makes.
        """
        filtered_person_df = self.df_person.filter(
            (F.col("PRSN_INJRY_SEV_ID") == "KILLED") & (F.col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")
        )
        joined_df = filtered_person_df.join(
            self.df_unit, ["CRASH_ID", "UNIT_NBR"], "inner"
        )
        result_df = joined_df.groupBy("VEH_MAKE_ID") \
            .count() \
            .orderBy(F.desc("count")) \
            .limit(5)
        return result_df

    def crash_analysis_4(self) -> int:
        """
        Count the number of vehicles with valid driver licenses involved in hit-and-run cases.

        Returns:
            int: Count of vehicles in hit-and-run cases.
        """
        filtered_person_df = self.df_person.filter(
            (F.col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE") | 
            (F.col("DRVR_LIC_TYPE_ID") == "COMMERCIAL DRIVER LIC.")
        )
        filtered_unit_df = self.df_unit.filter(F.col("VEH_HNR_FL") == "Y")
        joined_df = filtered_person_df.join(
            filtered_unit_df, ["CRASH_ID", "UNIT_NBR"], "inner"
        )
        return joined_df.count()

    def crash_analysis_5(self) -> DataFrame:
        """
        Find the state with the highest number of crashes where females were not involved.

        Returns:
            DataFrame: State with the highest number of crashes.
        """
        result_df = self.df_person.filter(F.col("PRSN_GNDR_ID") != "FEMALE") \
            .groupBy("DRVR_LIC_STATE_ID") \
            .count() \
            .orderBy(F.desc("count")) \
            .limit(1)
        return result_df

    def crash_analysis_6(self) -> DataFrame:
        """
        Get the 3rd to 5th top vehicle makes contributing to injuries including death.

        Returns:
            DataFrame: 3rd to 5th vehicle makes by crash count.
        """
        filtered_person_df = self.df_person.filter(
            (F.col("TOT_INJRY_CNT") > 0) | (F.col("DEATH_CNT") > 0)
        )
        joined_df = filtered_person_df.join(
            self.df_unit, ["CRASH_ID", "UNIT_NBR"], "inner"
        )
        ranked_df = joined_df.groupBy("VEH_MAKE_ID") \
            .count() \
            .orderBy(F.desc("count"))
        
        window_spec = Window.orderBy(F.desc("count"))
        ranked_df_with_row_num = ranked_df.withColumn("row_num", F.row_number().over(window_spec))

        result_df = ranked_df_with_row_num.filter((F.col("row_num") >= 3) & (F.col("row_num") <= 5)) \
            .drop("row_num")

        return result_df  

    def crash_analysis_7(self) -> DataFrame:
        """
        Determine the top ethnic user group for each vehicle body style.

        Returns:
            DataFrame: Top ethnic group for each vehicle body style.
        """
        joined_df = self.df_person.join(self.df_unit, ["CRASH_ID", "UNIT_NBR"], "inner")
        aggregated_df = joined_df.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID") \
            .count()
        window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(F.desc("count"))
        result_df = aggregated_df.withColumn("rank", F.row_number().over(window_spec)) \
            .filter(F.col("rank") == 1) \
            .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID", "count")
        return result_df

    def crash_analysis_8(self) -> DataFrame:
        """
        Find the top 5 ZIP codes with the highest number of crashes involving alcohol.

        Returns:
            DataFrame: Top 5 ZIP codes.
        """
        filtered_unit_df = self.df_unit.filter(
            (F.col("UNIT_DESC_ID") == "MOTOR VEHICLE") & 
            ((F.col("CONTRIB_FACTR_1_ID") == "UNDER INFLUENCE - ALCOHOL") | 
             (F.col("CONTRIB_FACTR_2_ID") == "HAD BEEN DRINKING"))
        )
        joined_df = self.df_person.join(
            filtered_unit_df, ["CRASH_ID", "UNIT_NBR"], "inner"
        )
        return joined_df.groupBy("DRVR_ZIP") \
            .count() \
            .orderBy(F.desc("count")) \
            .limit(5)

    def crash_analysis_9(self) -> int:
        """
        Count distinct crashes where no property was damaged, damage level > 4, and car has insurance.

        Returns:
            int: Count of crashes meeting the criteria.
        """
        damage_filter = self.df_unit.withColumn(
            "DMAG_SCL_NUM_1", 
            F.regexp_extract(F.col("VEH_DMAG_SCL_1_ID"), r"(\d+)", 1).cast("int")
        ).withColumn(
            "DMAG_SCL_NUM_2",
            F.regexp_extract(F.col("VEH_DMAG_SCL_2_ID"), r"(\d+)", 1).cast("int")
        )
        filtered_unit_df = damage_filter.filter(
            ((F.col("DMAG_SCL_NUM_1") > 4) | (F.col("DMAG_SCL_NUM_2") > 4)) & (F.col("FIN_RESP_TYPE_ID") != "NA")
        )
        result_df = filtered_unit_df.join(
            self.df_damages, "CRASH_ID", "left_anti"
        ).select("CRASH_ID").distinct()
        return result_df.count()

    def crash_analysis_10(self) -> DataFrame:
        """
        Get the top 5 vehicle makes where drivers were charged with speeding-related offenses.

        Returns:
            DataFrame: Top 5 vehicle makes.
        """
        filtered_charge_df = self.df_charges.filter(F.col("CHARGE").contains("SPEED"))
        filtered_person_df = self.df_person.filter(
            (F.col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE") | 
            (F.col("DRVR_LIC_TYPE_ID") == "COMMERCIAL DRIVER LIC.")
        )
        top_colors_df = self.df_unit.groupBy("VEH_COLOR_ID").count() \
            .orderBy(F.desc("count")).limit(10)
        top_states_df = self.df_unit.groupBy("VEH_LIC_STATE_ID").count() \
            .orderBy(F.desc("count")).limit(25)

        joined_df = self.df_unit \
            .join(filtered_charge_df, "CRASH_ID", "inner") \
            .join(filtered_person_df, ["CRASH_ID", "UNIT_NBR"], "inner") \
            .join(top_colors_df, "VEH_COLOR_ID", "inner") \
            .join(top_states_df, "VEH_LIC_STATE_ID", "inner")

        return joined_df.groupBy("VEH_MAKE_ID") \
            .count() \
            .orderBy(F.desc("count")) \
            .limit(5)
