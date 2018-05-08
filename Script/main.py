import sys

from police_reports import get_rdd as get_accident_rdd
from three_one_one import get_rdd as get_three_one_one_rdd
from vehicle_volume_count import get_rdd as get_vehicle_rdd

def main(sc, sqlContext, path_to_accident_csv, path_to_three_one_one_csv, path_to_count_csv, download=False):
    accident_rdd      = get_accident_rdd(sc, path_to_accident_csv, download=download, output_path="Raw Results")
    three_one_one_rdd = get_three_one_one_rdd(sc, path_to_three_one_one_csv, download=download, output_path="Raw Results")
    count_rdd = get_vehicle_rdd(sc, path_to_count_csv, download=download, output_path="Raw Results")

    joined_rdd        = count_rdd.join(accident_rdd.join(three_one_one_rdd))

    def mapper(tuples):
        for t in tuples:
            zip_code = t[0]
            traffic_count = t[1][0]
            acc_t = t[1][1][0]
            three_t = t[1][1][1]
            tuple_type = type(())

            if type(three_t) != tuple_type  or type(acc_t) != tuple_type:
                continue
            # accident dataset
            t_accidents  = acc_t[0]
            t_vehicles   = acc_t[1]
            t_per_i      = acc_t[2]
            t_per_k      = acc_t[3]
            t_ped_i      = acc_t[4]
            t_ped_k      = acc_t[5]
            t_cyc_i      = acc_t[6]
            t_cyc_k      = acc_t[7]
            t_mot_i      = acc_t[8]
            t_mot_k      = acc_t[9]
            fac1         = acc_t[10]
            fac2         = acc_t[11]
            fac3         = acc_t[12]
            fac4         = acc_t[13]
            fac5         = acc_t[14]
            veh1         = acc_t[15]
            veh2         = acc_t[16]
            veh3         = acc_t[17]
            veh4         = acc_t[18]
            veh5         = acc_t[19]
            # 311 dataset
            t_complaints = three_t[0]
            comp1        = three_t[1]
            comp2        = three_t[2]
            comp3        = three_t[3]
            desc1        = three_t[4]
            desc2        = three_t[5]
            desc3        = three_t[6]
            desc4        = three_t[7]
            desc5        = three_t[8]
            city         = three_t[9]

            accidents_per_1000_vehicle = round(t_accidents / float(traffic_count) * 1000, 2)
            injuries_per_1000_accident = round(t_per_i / float(t_accidents) * 1000, 2)
            deaths_per_1000_accident   = round(t_per_k / float(t_accidents) * 1000, 2)
            vehicles_involved_per_accident = round(t_vehicles / float(t_accidents), 2)
            yield (city, zip_code, accidents_per_1000_vehicle, injuries_per_1000_accident, deaths_per_1000_accident, vehicles_involved_per_accident,
                  t_accidents, t_vehicles, fac1, fac2, fac3, fac4, fac5, veh1, veh2, veh3, veh4, veh5,
                  comp1, comp2, comp3, desc1, desc2, desc3, desc4, desc5, t_complaints)

    full_rdd = joined_rdd.mapPartitions(mapper)

    from pyspark.sql.types import StringType, FloatType, IntegerType, StructField, StructType
    fields = [StructField("", StringType(), True) for _ in range(0, 27)]
    fields[0].name = "City"

    fields[1].name = "Zip_Code"

    fields[2].dataType = FloatType()
    fields[2].name = "Accidents_Per_1000_Vehicles"

    fields[3].dataType = FloatType()
    fields[3].name = "Injuries_Per_1000_Accidents"

    fields[4].dataType = FloatType()
    fields[4].name = "Deaths_Per_1000_Accidents"

    fields[5].dataType = FloatType()
    fields[5].name = "Vehicles_Involved_Per_Accident"

    fields[6].dataType = IntegerType()
    fields[6].name = "Total_Number_of_Accidents"

    fields[7].dataType = IntegerType()
    fields[7].name = "Total_Number_of_Vehicles_Involved_in_Accidents"

    fields[8].name  = "Top_Factor_1"
    fields[9].name  = "Top_Factor_2"
    fields[10].name  = "Top_Factor_3"
    fields[11].name = "Top_Factor_4"
    fields[12].name = "Top_Factor_5"

    fields[13].name = "Most_Common_Vehicle_Type_1"
    fields[14].name = "Most_Common_Vehicle_Type_2"
    fields[15].name = "Most_Common_Vehicle_Type_3"
    fields[16].name = "Most_Common_Vehicle_Type_4"
    fields[17].name = "Most_Common_Vehicle_Type_5"

    fields[18].name = "Top_Street_Complaint_1"
    fields[19].name = "Top_Street_Complaint_2"
    fields[20].name = "Top_Street_Complaint_3"

    fields[21].name = "Top_Complaint_Description_1"
    fields[22].name = "Top_Complaint_Description_2"
    fields[23].name = "Top_Complaint_Description_3"
    fields[24].name = "Top_Complaint_Description_4"
    fields[25].name = "Top_Complaint_Description_5"

    fields[26].dataType = IntegerType()
    fields[26].name = "Total Street Complaints"

    schema = StructType(fields)

    full_df = sqlContext.createDataFrame(full_rdd, schema)

    temp_df = full_df.describe(['Accidents_Per_1000_Vehicles', 'Injuries_Per_1000_Accidents', 'Deaths_Per_1000_Accidents', 'Vehicles_Involved_Per_Accident'])
    new_df = temp_df.select(temp_df.summary,
                            temp_df.Accidents_Per_1000_Vehicles.cast(FloatType()).alias('Accidents'),
                            temp_df.Injuries_Per_1000_Accidents.cast(FloatType()).alias('Injuries'),
                            temp_df.Deaths_Per_1000_Accidents.cast(FloatType()).alias('Deaths'),
                            temp_df.Vehicles_Involved_Per_Accident.cast(FloatType()).alias('Vehicles'))

    temp_lst = new_df.collect()

    avg_row = temp_lst[1]
    avg_accidents_per_1000             = round(avg_row.Accidents, 4)
    avg_injuries_per_1000              = round(avg_row.Injuries, 4)
    avg_deaths_per_1000                = round(avg_row.Deaths, 4)
    avg_vehicles_involved_per_accident = round(avg_row.Vehicles, 4)
    std_dev_row = temp_lst[2]
    std_dev_accidents_per_1000             = round(std_dev_row.Accidents, 4)
    std_dev_injuries_per_1000              = round(std_dev_row.Injuries, 4)
    std_dev_deaths_per_1000                = round(std_dev_row.Deaths, 4)
    std_dev_vehicles_involved_per_accident = round(std_dev_row.Vehicles, 4)

    one_std_dev_accidents   = std_dev_accidents_per_1000
    two_std_dev_accidents   = 2 * one_std_dev_accidents
    three_std_dev_accidents = 3 * one_std_dev_accidents

    one_std_dev_injuries    = std_dev_injuries_per_1000
    two_std_dev_injuries    = 2 * one_std_dev_injuries
    three_std_dev_injuries  = 3 * one_std_dev_injuries

    one_std_dev_deaths      = std_dev_deaths_per_1000
    two_std_dev_deaths      = 2 * one_std_dev_deaths
    three_std_dev_deaths    = 3 * one_std_dev_deaths

    one_std_dev_vehicles    = std_dev_vehicles_involved_per_accident
    two_std_dev_vehicles    = 2 * one_std_dev_vehicles
    three_std_dev_vehicles  = 3 * one_std_dev_vehicles

    top_10_accidents_per_1000_vehicles       = map(lambda x: x.asDict(), full_df.sort('Accidents_Per_1000_Vehicles', ascending=False).take(10))
    top_10_injuries_per_1000_accidents       = map(lambda x: x.asDict(), full_df.sort('Injuries_Per_1000_Accidents', ascending=False).take(10))
    top_10_deaths_per_1000_accidents         = map(lambda x: x.asDict(), full_df.sort('Deaths_Per_1000_Accidents', ascending=False).take(10))
    top_10_vehicles_involved_per_accident    = map(lambda x: x.asDict(), full_df.sort('Vehicles_Involved_Per_Accident', ascending=False).take(10))

    bottom_10_accidents_per_1000_vehicles    = map(lambda x: x.asDict(), full_df.sort('Accidents_Per_1000_Vehicles', ascending=True).take(10))
    bottom_10_injuries_per_1000_accidents    = map(lambda x: x.asDict(), full_df.sort('Injuries_Per_1000_Accidents', ascending=True).take(10))
    bottom_10_deaths_per_1000_accidents      = map(lambda x: x.asDict(), full_df.sort('Deaths_Per_1000_Accidents', ascending=True).take(10))
    bottom_10_vehicles_involved_per_accident = map(lambda x: x.asDict(), full_df.sort('Vehicles_Involved_Per_Accident', ascending=True).take(10))

    def classify_mapper(records):
        for record in records:
            city = record[0]
            zip_code = record[1]
            accidents_per_1000 = record[2]
            injuries_per_1000 = record[3]
            deaths_per_1000 = record[4]
            vehicles_involved_per_accident = record[5]

            upper_a_bound = round(avg_accidents_per_1000 + one_std_dev_accidents, 4)
            lower_a_bound = round(avg_accidents_per_1000 - one_std_dev_accidents, 4)

            upper_i_bound = round(avg_injuries_per_1000 + one_std_dev_injuries, 4)
            lower_i_bound = round(avg_injuries_per_1000 - one_std_dev_injuries, 4)

            upper_d_bound = round(avg_deaths_per_1000 + one_std_dev_deaths, 4)
            lower_d_bound = round(avg_deaths_per_1000 - one_std_dev_deaths, 4)

            zip_safety_level = ""

            if accidents_per_1000 > upper_a_bound:
                if injuries_per_1000 < lower_i_bound and deaths_per_1000 < lower_d_bound:
                    print ("{} is classified as Ok with {} A {} I {} D ".format(zip_code, accidents_per_1000, injuries_per_1000, deaths_per_1000))
                    zip_safety_level = "Ok"
                else:
                    print ("{} is classified as Hazardous with {} accidents per 1000 vehicles".format(zip_code, accidents_per_1000))
                    zip_safety_level = "Hazardous"
            elif accidents_per_1000 < lower_a_bound:
                if injuries_per_1000 > upper_i_bound and deaths_per_1000 > upper_d_bound:
                    print ("{} is classified as Ok with {} A {} I {} D ".format(zip_code, accidents_per_1000, injuries_per_1000, deaths_per_1000))
                    zip_safety_level = "Ok"
                else:
                    print("{} is classified as Safe with {} accidents per 1000 vehicles".format(zip_code, accidents_per_1000))
                    zip_safety_level = "Safe"
            else:
                # accidents_per_1000 falls within one std deviation of mean
                if injuries_per_1000 > upper_i_bound and deaths_per_1000 > upper_d_bound:
                    print("{} is classified as Hazardous with {} A {} I {} D ".format(zip_code, accidents_per_1000, injuries_per_1000, deaths_per_1000))
                    zip_safety_level = "Hazardous"
                elif injuries_per_1000 < lower_i_bound and deaths_per_1000 < lower_d_bound:
                    print("{} is classified as Safe with {} A {} I {} D ".format(zip_code, accidents_per_1000, injuries_per_1000, deaths_per_1000))
                    zip_safety_level = "Safe"
                else:
                    print ("{} is classified as Ok with {} accidents per 1000 vehicles".format(zip_code, accidents_per_1000))
                    zip_safety_level = "Ok"


            yield (city, zip_code, zip_safety_level,
                  accidents_per_1000, injuries_per_1000, deaths_per_1000, vehicles_involved_per_accident,
                  record[6], record[7], record[8], record[9], record[10], record[11], record[12], record[13],
                  record[14], record[15], record[16], record[17], record[18], record[19], record[20], record[21],
                  record[22], record[23], record[24], record[25], record[26])

    def add_header(records):
        for record in records:
            if record == first:
                yield header
                yield record
            else:
                yield record

    def to_csv_line(data):
        return ','.join(str(d) for d in data)

    final_rdd = full_rdd.mapPartitions(classify_mapper)
    first = final_rdd.first()
    header = ["City, Zip_Code, Safety_classifier, Accidents_per_1000_veh, Injuries_per_1000_accidents, Deaths_per_1000_accidents, Vehicles_involved_per_accident, Total_num_accidents, Total_num_of_accident_veh, Top_factor_1, Top_factor_2, Top_factor_3, Top_factor_4, Top_factor_5, Top_vehicle_type_1, Top_vehicle_type_2, Top_vehicle_type_3, Top_vehicle_type_4, Top_vehicle_type_5, Top_street_complaint_1, Top_street_complaint_2, Top_street_complaint_3, Top_complaint_desc_1, Top_complaint_desc_2, Top_complaint_desc_3, Top_complaint_desc_4, Top_complaint_desc_5, Num_street_complaints"]

    final_rdd.mapPartitions(add_header).map(to_csv_line).coalesce(1).saveAsTextFile("Raw Results/results/final_result")

    sc.parallelize(top_10_accidents_per_1000_vehicles).coalesce(1).saveAsTextFile("Raw Results/totals/top_ten/top_ten_accidents_per_1000_vehicles")
    sc.parallelize(top_10_injuries_per_1000_accidents).coalesce(1).saveAsTextFile("Raw Results/totals/top_ten/top_ten_injuries_per_1000_accidents")
    sc.parallelize(top_10_deaths_per_1000_accidents).coalesce(1).saveAsTextFile("Raw Results/totals/top_ten/top_ten_deaths_per_1000_accidents")
    sc.parallelize(top_10_vehicles_involved_per_accident).coalesce(1).saveAsTextFile("Raw Results/totals/top_ten/top_ten_vehicles_involved_per_accident")

    sc.parallelize(bottom_10_accidents_per_1000_vehicles).coalesce(1).saveAsTextFile("Raw Results/totals/bottom_ten/bottom_ten_accidents_per_1000_vehicles")
    sc.parallelize(bottom_10_injuries_per_1000_accidents).coalesce(1).saveAsTextFile("Raw Results/totals/bottom_ten/bottom_ten_injuries_per_1000_accidents")
    sc.parallelize(bottom_10_deaths_per_1000_accidents).coalesce(1).saveAsTextFile("Raw Results/totals/bottom_ten/bottom_ten_deaths_per_1000_accidents")
    sc.parallelize(bottom_10_vehicles_involved_per_accident).coalesce(1).saveAsTextFile("Raw Results/totals/bottom_ten/bottom_ten_vehicles_involved_per_accident")

    aggregation = map(lambda x: x.asDict(), new_df.select(new_df.summary,
                                                          new_df.Accidents.alias("Accidents per 1000 vehicles"),
                                                          new_df.Injuries.alias("Injuries per 1000 accidents"),
                                                          new_df.Deaths.alias("Deaths per 1000 accidents"),
                                                          new_df.Vehicles.alias("Vehicles involved per accident")).collect())

    sc.parallelize(aggregation).coalesce(1).saveAsTextFile("Raw Results/totals/aggregations")

if __name__ == "__main__":
    import pyspark
    from pyspark.sql import SQLContext
    sc = pyspark.SparkContext()
    sqlContext = pyspark.SQLContext(sc)
    try:
        if sys.argv[4].lower() == 'download':
            download = True
        else:
            download=False
    except:
        download = False
    main(sc, sqlContext, sys.argv[1], sys.argv[2], sys.argv[3], download=download)
