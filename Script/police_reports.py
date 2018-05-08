
def get_rdd(sc, accident_csv, download=False, output_path=None):
    
    from datetime import datetime
    from dateutil.parser import parse
    from heapq import nlargest
    accident_rdd = sc.textFile(accident_csv, use_unicode=False)
    def my_format(records):
        first_day_2012 = datetime(year=2012, month=1, day=1, hour=0, minute=0)
        last_day_2013 = datetime(year=2013, month=12, day=31, hour=23, minute=59)
        for record in records:
            if "ZIP CODE" in record:
                continue
            fields = record.split(",")
            if (len(fields) == 30 or len(fields) == 29):
                # Ignore incorrectly formatted records (5-10 records have stray commas in fields)
                location_record = ''
                # date as datetime.date
                date_string = fields[0]
                time_string = fields[1]
                zip_code = fields[3]
                datetime_record = parse("{} {}".format(date_string, time_string))
                if not first_day_2012 <= datetime_record <= last_day_2013 or zip_code == "":
                    continue
                if (len(fields) == 30):
                    per_i = int(fields[11])
                    per_k = int(fields[12])
                    peds_i = int(fields[13])
                    peds_k = int(fields[14])
                    cyc_i = int(fields[15])
                    cyc_k = int(fields[16])
                    mot_i = int(fields[17])
                    mot_k = int(fields[18])
                    fac_1 = fields[19]
                    fac_2 = fields[20]
                    fac_3 = fields[21]
                    fac_4 = fields[22]
                    fac_5 = fields[23]
                    veh_1 = fields[25]
                    veh_2 = fields[26]
                    veh_3 = fields[27]
                    veh_4 = fields[28]
                    veh_5 = fields[29]
                else:
                    per_i = int(fields[10])
                    per_k = int(fields[11])
                    peds_i = int(fields[12])
                    peds_k = int(fields[13])
                    cyc_i = int(fields[14])
                    cyc_k = int(fields[15])
                    mot_i = int(fields[16])
                    mot_k = int(fields[17])
                    fac_1 = fields[18]
                    fac_2 = fields[19]
                    fac_3 = fields[20]
                    fac_4 = fields[21]
                    fac_5 = fields[22]
                    veh_1 = fields[24]
                    veh_2 = fields[25]
                    veh_3 = fields[26]
                    veh_4 = fields[27]
                    veh_5 = fields[28]

                yield (zip_code, (per_i, per_k, peds_i, peds_k, cyc_i, cyc_k, mot_i, mot_k, fac_1,
                              fac_2, fac_3, fac_4, fac_5,
                              veh_1, veh_2, veh_3, veh_4, veh_5))

    accident_zip_rdd = accident_rdd.mapPartitions(my_format)

    def seqFunc(agg_dict, record):
        """
        For each ZIP code, calculate statistics
        """
        per_i = record[0]
        per_k = record[1]
        peds_i = record[2]
        peds_k = record[3]
        cyc_i = record[4]
        cyc_k = record[5]
        mot_i = record[6]
        mot_k = record[7]
        fac_1 = record[8]
        fac_2 = record[9]
        fac_3 = record[10]
        fac_4 = record[11]
        fac_5 = record[12]
        veh_1 = record[13]
        veh_2 = record[14]
        veh_3 = record[15]
        veh_4 = record[16]
        veh_5 = record[17]

        agg_dict['total_accidents'] = agg_dict.get('total_accidents', 0) + 1
        agg_dict['total_per_i'] = agg_dict.get('total_per_i', 0) + per_i
        agg_dict['total_per_k'] = agg_dict.get('total_per_k', 0) + per_k
        agg_dict['total_ped_i'] = agg_dict.get('total_ped_i', 0) + peds_i
        agg_dict['total_ped_k'] = agg_dict.get('total_ped_k', 0) + peds_k
        agg_dict['total_cyc_i'] = agg_dict.get('total_cyc_i', 0) + cyc_i
        agg_dict['total_cyc_k'] = agg_dict.get('total_cyc_k', 0) + cyc_k
        agg_dict['total_mot_i'] = agg_dict.get('total_mot_i', 0) + mot_i
        agg_dict['total_mot_k'] = agg_dict.get('total_mot_k', 0) + mot_k

        if fac_1 != "":
            agg_dict[fac_1] = agg_dict.get(fac_1, 0) + 1
        if fac_2 != "":
            agg_dict[fac_2] = agg_dict.get(fac_2, 0) + 1
        if fac_3 != "":
            agg_dict[fac_3] = agg_dict.get(fac_3, 0) + 1
        if fac_4 != "":
            agg_dict[fac_4] = agg_dict.get(fac_4, 0) + 1
        if fac_5 != "":
            agg_dict[fac_5] = agg_dict.get(fac_5, 0) + 1

        if veh_1 != "":
            agg_dict[veh_1] = agg_dict.get(veh_1, 0) + 1
            agg_dict['total_vehicles'] = agg_dict.get('total_vehicles', 0) + 1
        if veh_2 != "":
            agg_dict[veh_2] = agg_dict.get(veh_2, 0) + 1
            agg_dict['total_vehicles'] = agg_dict.get('total_vehicles', 0) + 1
        if veh_3 != "":
            agg_dict[veh_3] = agg_dict.get(veh_3, 0) + 1
            agg_dict['total_vehicles'] = agg_dict.get('total_vehicles', 0) + 1
        if veh_4 != "":
            agg_dict[veh_4] = agg_dict.get(veh_4, 0) + 1
            agg_dict['total_vehicles'] = agg_dict.get('total_vehicles', 0) + 1
        if veh_5 != "":
            agg_dict[veh_5] = agg_dict.get(veh_5, 0) + 1
            agg_dict['total_vehicles'] = agg_dict.get('total_vehicles', 0) + 1

        return agg_dict

    def combFunc(dict1, dict2):
        for key, value in dict1.items():
            dict2[key] = dict2.get(key, 0) + value
        return dict2

    result_rdd = accident_zip_rdd.aggregateByKey({}, seqFunc, combFunc)

    def combine(agg_dict, record_dict):
        """
        Combine statistics for all ZIP codes to get total statistics
        """
        for key, value in record_dict.items():
            agg_dict[key] = agg_dict.get(key, 0) + value
        return agg_dict


    aggregated_dict = result_rdd.map(lambda x: x[1]) \
                                .fold({}, combine)
    totals = sorted(aggregated_dict.items(), key=lambda x: x[1], reverse=True)
    """
    Create lists of all vehicle types and accident factors in the dataset for later use
    """
    ignore_categories = ['total_vehicles', 'total_accidents', 'total_per_i', 'total_mot_i', 'total_ped_i', 'total_cyc_i',
                        'total_per_k', 'total_ped_k', 'total_mot_k', 'total_cyc_k', 'UNKNOWN', 'Unspecified']
    vehicle_types = map(lambda x: x[0], filter(lambda x: x[0].isupper() and x[0] not in ignore_categories, totals))
    accident_factors = map(lambda x: x[0], filter(lambda x: not x[0].isupper() and x[0] not in ignore_categories, totals))



    def topn(zip_tuples):
        """
        For each zip code, use dictionary to produce final accident statistics
        """
        for zip_tuple in zip_tuples:
            zip_code = zip_tuple[0]
            zip_dict = zip_tuple[1]
            total_vehicles = float(zip_dict['total_vehicles'])
            total_accidents = float(zip_dict['total_accidents'])
            total_per_i = float(zip_dict['total_per_i'])
            total_per_k = float(zip_dict['total_per_k'])
            if total_per_i == 0:
                per_ped_i = 0
                per_cyc_i = 0
                per_mot_i = 0
            else:
                per_ped_i = zip_dict['total_ped_i']/ total_per_i * 100
                per_cyc_i = zip_dict['total_cyc_i']/ total_per_i * 100
                per_mot_i = zip_dict['total_mot_i']/ total_per_i * 100
            if total_per_k == 0:
                per_ped_k = 0
                per_cyc_k = 0
                per_mot_k = 0
            else:
                per_ped_k = zip_dict['total_ped_k']/ total_per_k * 100
                per_cyc_k = zip_dict['total_cyc_k']/ total_per_k * 100
                per_mot_k = zip_dict['total_mot_k']/ total_per_k * 100

            ped_i = "{}: {}%".format(zip_dict['total_ped_i'], round(per_ped_i), 2)
            ped_k = "{}: {}%".format(zip_dict['total_ped_k'], round(per_ped_k), 2)
            cyc_i = "{}: {}%".format(zip_dict['total_cyc_i'], round(per_cyc_i), 2)
            cyc_k = "{}: {}%".format(zip_dict['total_cyc_k'], round(per_cyc_k), 2)
            mot_i = "{}: {}%".format(zip_dict['total_mot_i'], round(per_mot_i), 2)
            mot_k = "{}: {}%".format(zip_dict['total_mot_k'], round(per_mot_k), 2)

            fac1 = ''
            fac2 = ''
            fac3 = ''
            fac4 = ''
            fac5 = ''

            veh1 = ''
            veh2 = ''
            veh3 = ''
            veh4 = ''
            veh5 = ''

            vehicle_type_dict = {k:v for (k,v) in zip_dict.items() if k in vehicle_types }
            accident_factors_dict = {k:v for (k,v) in zip_dict.items() if k in accident_factors }
            v_dict_length = len(vehicle_type_dict)
            a_dict_length = len(accident_factors_dict)
            if v_dict_length < 3 and v_dict_length != 0:
                top_n_vehicles = nlargest(1, vehicle_type_dict.items(), key=lambda x: x[1])
                veh1 = "{}: {}: {}%".format(top_n_vehicles[0][0], top_n_vehicles[0][1], round(top_n_vehicles[0][1]/total_vehicles * 100, 2))
            elif v_dict_length < 5 and v_dict_length != 0:
                top_n_vehicles = nlargest(3, vehicle_type_dict.items(), key=lambda x: x[1])
                veh1 = "{}: {}: {}%".format(top_n_vehicles[0][0], top_n_vehicles[0][1], round(top_n_vehicles[0][1]/total_vehicles * 100, 2))
                veh2 = "{}: {}: {}%".format(top_n_vehicles[1][0], top_n_vehicles[1][1], round(top_n_vehicles[1][1]/total_vehicles * 100, 2))
                veh3 = "{}: {}: {}%".format(top_n_vehicles[2][0], top_n_vehicles[2][1], round(top_n_vehicles[2][1]/total_vehicles * 100, 2))
            elif v_dict_length != 0:
                top_n_vehicles = nlargest(5, vehicle_type_dict.items(), key=lambda x: x[1])
                veh1 = "{}: {}: {}%".format(top_n_vehicles[0][0], top_n_vehicles[0][1], round(top_n_vehicles[0][1]/total_vehicles * 100, 2))
                veh2 = "{}: {}: {}%".format(top_n_vehicles[1][0], top_n_vehicles[1][1], round(top_n_vehicles[1][1]/total_vehicles * 100, 2))
                veh3 = "{}: {}: {}%".format(top_n_vehicles[2][0], top_n_vehicles[2][1], round(top_n_vehicles[2][1]/total_vehicles * 100, 2))
                veh4 = "{}: {}: {}%".format(top_n_vehicles[3][0], top_n_vehicles[3][1], round(top_n_vehicles[3][1]/total_vehicles * 100, 2))
                veh5 = "{}: {}: {}%".format(top_n_vehicles[4][0], top_n_vehicles[4][1], round(top_n_vehicles[4][1]/total_vehicles * 100, 2))

            if a_dict_length < 3 and a_dict_length != 0:
                top_n_factors = nlargest(1, accident_factors_dict.items(), key=lambda x: x[1])
                fac1 = "{}: {}: {}%".format(top_n_factors[0][0], top_n_factors[0][1], round(top_n_factors[0][1]/total_vehicles * 100, 2))
            elif a_dict_length < 5 and a_dict_length != 0:
                top_n_factors = nlargest(3, accident_factors_dict.items(), key=lambda x: x[1])
                fac1 = "{}: {}: {}%".format(top_n_factors[0][0], top_n_factors[0][1], round(top_n_factors[0][1]/total_vehicles * 100, 2))
                fac2 = "{}: {}: {}%".format(top_n_factors[1][0], top_n_factors[1][1], round(top_n_factors[1][1]/total_vehicles * 100, 2))
                fac3 = "{}: {}: {}%".format(top_n_factors[2][0], top_n_factors[2][1], round(top_n_factors[2][1]/total_vehicles * 100, 2))
            elif a_dict_length != 0:
                top_n_factors = nlargest(5, accident_factors_dict.items(), key=lambda x: x[1])
                fac1 = "{}: {}: {}%".format(top_n_factors[0][0], top_n_factors[0][1], round(top_n_factors[0][1]/total_vehicles * 100, 2))
                fac2 = "{}: {}: {}%".format(top_n_factors[1][0], top_n_factors[1][1], round(top_n_factors[1][1]/total_vehicles * 100, 2))
                fac3 = "{}: {}: {}%".format(top_n_factors[2][0], top_n_factors[2][1], round(top_n_factors[2][1]/total_vehicles * 100, 2))
                fac4 = "{}: {}: {}%".format(top_n_factors[3][0], top_n_factors[3][1], round(top_n_factors[3][1]/total_vehicles * 100, 2))
                fac5 = "{}: {}: {}%".format(top_n_factors[4][0], top_n_factors[4][1], round(top_n_factors[4][1]/total_vehicles * 100, 2))

            yield (zip_code,
                  (int(total_accidents), int(total_vehicles),
                  int(total_per_i), int(total_per_k),
                  ped_i, ped_k,
                  cyc_i, cyc_k,
                  mot_i, mot_k,
                  fac1, fac2, fac3, fac4, fac5,
                  veh1, veh2, veh3, veh4, veh5))

    zip_statistics_rdd = result_rdd.mapPartitions(topn)

    if download:
        def add_header(records):
            for record in records:
                if record == first_totals:
                    yield totals_header
                    yield record
                elif record == first_results:
                    yield results_header
                    yield record
                else:
                    yield record

        def to_csv_line(data):
            return ','.join(str(d) for d in data)

        first_results = zip_statistics_rdd.first()
        print("FIRST RESULTS: {}".format(first_results))
        totals_rdd = sc.parallelize(totals)
        first_totals = totals_rdd.first()
        print("FIRST TOTALS: {}".format(first_totals))
        totals_header = ["Category, Count"]
        results_header = ["Zip_code, Total_accidents, Total_vehicles_in_accidents, Total_persons_injured, Total_persons_killed, Total_pedestrians_injured, Total_pedestrians_killed, Total_cyclists_injured, Total_cyclists_killed, Total_motorists_injured, Total_motorists_killed, Top_factor_1, Top_factor_2, Top_factor_3, Top_factor_4, Top_factor_5, Top_vehicle_type_1, Top_vehicle_type_2, Top_vehicle_type_3, Top_vehicle_type_4, Top_vehicle_type_5"]

        zip_statistics_rdd.mapPartitions(add_header).map(to_csv_line).coalesce(1).saveAsTextFile(output_path + '/Raw Results/police_report_results')
        totals_rdd.mapPartitions(add_header).map(to_csv_line).coalesce(1).saveAsTextFile(output_path + '/totals/NYPD_accident_2012_2013')

    return zip_statistics_rdd
