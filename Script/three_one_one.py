
def get_rdd(sc, three_one_one_csv, download=False, output_path=None):

    from datetime import datetime
    from dateutil.parser import parse
    from heapq import nlargest
    rdd = sc.textFile(three_one_one_csv)
    header = rdd.first()

    def select_fields(records):
        first_day_2012 = datetime(year=2012, month=1, day=1, hour=0, minute=0)
        last_day_2013 = datetime(year=2013, month=12, day=31, hour=23, minute=59)
        for record in records:
            if record == header:
                continue
            fields = record.split(',')
            complaint_type = fields[5]
            zip_code = fields[8]
            city = fields[16]
            try:
                date_parsed = parse(fields[1])
            except ValueError:
                continue
            try:
                zip_code = int(fields[8])
                descriptor = fields[6]
            except ValueError:
                if zip_code == "":
                    continue
                elif "Street Sign" in complaint_type:
                    zip_code = fields[10]
                    descriptor = "{} {} {}".format(fields[6], fields[7], fields[8])
                    city = fields[18]
                else:
                    zip_code = fields[9]
                    descriptor = "{} {}".format(fields[6], fields[7])
                    city = fields[17]

            if (first_day_2012 <= date_parsed <= last_day_2013 and zip_code != "" and city != "" and "Street" in complaint_type and "Noise" not in complaint_type):
                yield (str(zip_code), (str(complaint_type).upper(), str(descriptor), str(city)))

    filtered_rdd = rdd.mapPartitions(select_fields)

    def seqOp(agg_dict, record):
        complaint_type = record[0]
        descriptor = record[1]
        agg_dict["total_complaints"] = agg_dict.get("total_complaints", 0) + 1
        agg_dict[complaint_type] = agg_dict.get(complaint_type, 0) + 1
        agg_dict[descriptor] = agg_dict.get(descriptor, 0) + 1
        agg_dict['city'] = record[2]
        return agg_dict

    def combOp (dict1, dict2):
        for key, value in dict1.items():
            if key != "city":
                dict2[key] = dict2.get(key, 0) + value
        return dict2

    zip_complaints_rdd = filtered_rdd.aggregateByKey({}, seqOp, combOp)

    def topn(zip_tuples):
        for zip_tuple in zip_tuples:
            zip_code = zip_tuple[0]
            zip_dict = zip_tuple[1]
            city = zip_dict["city"]
            total_complaints = float(zip_dict['total_complaints'])
            complaints_dict = {k:v for (k,v) in zip_dict.items() if k.isupper() }
            descriptors_dict = {k:v for (k,v) in zip_dict.items() if not k.isupper() and k != 'total_complaints' and k != 'city'}
            # comp1 comp2 comp3 represent top three complaints for zip code
            # desc1 desc2 desc3 desc4 desc5 represent top three descriptors of complaints for zip code
            comp1 = ''
            comp2 = ''
            comp3 = ''
            desc1 = ''
            desc2 = ''
            desc3 = ''
            desc4 = ''
            desc5 = ''
            c_dict_len = len(complaints_dict)
            d_dict_len = len(descriptors_dict)
            if c_dict_len < 3 and c_dict_len != 0:
                top_complaint = nlargest(1, complaints_dict.items(), key=lambda x: x[1])
                comp1 = "{}: {}: {}%".format(top_complaint[0][0], top_complaint[0][1], round(top_complaint[0][1]/total_complaints * 100, 2))
            elif c_dict_len != 0:
                top_n_complaints = nlargest(3, complaints_dict.items(), key=lambda x: x[1])
                comp1 = "{}: {}: {}%".format(top_n_complaints[0][0], top_n_complaints[0][1], round(top_n_complaints[0][1]/total_complaints * 100, 2))
                comp2 = "{}: {}: {}%".format(top_n_complaints[1][0], top_n_complaints[1][1], round(top_n_complaints[1][1]/total_complaints * 100 ,2))
                comp3 = "{}: {}: {}%".format(top_n_complaints[2][0], top_n_complaints[2][1], round(top_n_complaints[2][1]/total_complaints * 100 ,2))
            if d_dict_len < 3 and d_dict_len != 0:
                top_n_descriptors = nlargest(1, descriptors_dict.items(), key=lambda x: x[1])
                desc1 = "{}: {}: {}%".format(top_n_descriptors[0][0], top_n_descriptors[0][1], round(top_n_descriptors[0][1]/total_complaints * 100, 2))
            elif d_dict_len < 5 and d_dict_len != 0:
                top_n_descriptors = nlargest(3, descriptors_dict.items(), key=lambda x: x[1])
                desc1 = "{}: {}: {}%".format(top_n_descriptors[0][0], top_n_descriptors[0][1], round(top_n_descriptors[0][1]/total_complaints * 100, 2))
                desc2 = "{}: {}: {}%".format(top_n_descriptors[1][0], top_n_descriptors[1][1], round(top_n_descriptors[1][1]/total_complaints * 100, 2))
                desc3 = "{}: {}: {}%".format(top_n_descriptors[2][0], top_n_descriptors[2][1], round(top_n_descriptors[2][1]/total_complaints * 100, 2))
            elif d_dict_len != 0:
                top_n_descriptors = nlargest(5, descriptors_dict.items(), key=lambda x: x[1])
                desc1 = "{}: {}: {}%".format(top_n_descriptors[0][0], top_n_descriptors[0][1], round(top_n_descriptors[0][1]/total_complaints * 100, 2))
                desc2 = "{}: {}: {}%".format(top_n_descriptors[1][0], top_n_descriptors[1][1], round(top_n_descriptors[1][1]/total_complaints * 100, 2))
                desc3 = "{}: {}: {}%".format(top_n_descriptors[2][0], top_n_descriptors[2][1], round(top_n_descriptors[2][1]/total_complaints * 100, 2))
                desc4 = "{}: {}: {}%".format(top_n_descriptors[3][0], top_n_descriptors[3][1], round(top_n_descriptors[3][1]/total_complaints * 100, 2))
                desc5 = "{}: {}: {}%".format(top_n_descriptors[4][0], top_n_descriptors[4][1], round(top_n_descriptors[4][1]/total_complaints * 100, 2))
            yield (zip_code, (int(total_complaints), comp1, comp2, comp3, desc1, desc2, desc3, desc4, desc5, city))

    def combine(agg_dict, record_dict):
        for key, value in record_dict.items():
            if key != "city":
                agg_dict[key] = agg_dict.get(key, 0) + value
        return agg_dict

    aggregate_dict = zip_complaints_rdd.map(lambda x: x[1]) \
                                       .fold({}, combine)
    totals = sorted(aggregate_dict.items(), key=lambda x: x[1], reverse=True)
    three_one_one_statistics_rdd = zip_complaints_rdd.mapPartitions(topn)

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

        first_results = three_one_one_statistics_rdd.first()
        first_totals = sc.parallelize(totals).first()

        totals_header = ["Category, Count"]
        results_header = ["Zip_code, Total_complaints, Top_complaint_1, Top_complaint_2, Top_complaint_3, Top_complaint_description_1, Top_complaint_description_2, Top_complaint_description_3, Top_complaint_description_4, Top_complaint_description_5, City"]
        three_one_one_statistics_rdd.mapPartitions(add_header, first_results).map(to_csv_line).coalesce(1).saveAsTextFile(output_path + "/Raw Results/311_results")
        sc.parallelize(totals).mapPartitions(add_header, first_totals).map(to_csv_line).coalesce(1).saveAsTextFile(output_path + "/Results/three_one_one_2012_2013")

    return three_one_one_statistics_rdd
