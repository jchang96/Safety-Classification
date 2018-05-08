
def get_rdd(sc, count_csv_path, download=False, output_path=None):

    zip_num_vehicles_rdd = sc.textFile(count_csv_path)
    header = zip_num_vehicles_rdd.first()
    def map_multiple_zip(records):
        """
        Some results are of form 11220;11204,39294, 34333 or 10309: 10312, 50033. Need to count as two separate records. One record is NY 10026,12496, duplicate
        of record before it
        """
        for record in records:
            if not record == header:
                fields = record.split(',')
                zip_code = fields[0]
                vehicle_count = int(fields[1])
                samples = int(fields[2])
                half_vehicle_count = vehicle_count / 2
                half_samples = samples / 2

                if "NY" in zip_code:
                    # Duplicate record with incorrect format
                    continue
                elif ';' in zip_code:
                    # Of form 11220;11204,39294
                    zip1 = zip_code.split(';')[0].strip()
                    zip2 = zip_code.split(';')[1].strip()
                    yield (zip1, (half_vehicle_count, half_samples, 1))
                    yield (zip2, (half_vehicle_count, half_samples, 1))
                elif ':' in zip_code:
                    # Of form 10309: 10312
                    zip1 = zip_code.split(':')[0].strip()
                    zip2 = zip_code.split(':')[1].strip()
                    half_vehicle_count = vehicle_count / 2
                    yield (zip1, (half_vehicle_count, half_samples, 1))
                    yield(zip2, (half_vehicle_count, half_samples, 1))
                else:
                    yield (zip_code, (vehicle_count, samples, 1))

    zip_key_rdd = zip_num_vehicles_rdd.mapPartitions(map_multiple_zip)

    def dict_mapper(zip_tuples):
        for zip_tuple in zip_tuples:
            zip_code = zip_tuple[0]
            zip_dict = zip_tuple[1]
            vehicle_count = zip_dict['vehicle_count']
            samples = zip_dict['samples']
            days_in_2012_2013 = 731 # 2012 was a leap year
            vehicle_count_2012_2013 = vehicle_count / samples * days_in_2012_2013
            yield (zip_code, vehicle_count_2012_2013)

    def seqOp(dict1, tup):
        vehicle_count = tup[0]
        samples = tup[1]
        dict1['vehicle_count'] = dict1.get('vehicle_count', 0) + vehicle_count
        dict1['samples'] = dict1.get('samples', 0) + samples
        return dict1

    def combOp(dict1, dict2):
        for key, value in dict1.items():
            dict2[key] = dict2.get(key, 0) + value
            return dict2

    result_rdd = zip_key_rdd.aggregateByKey({}, seqOp, combOp).mapPartitions(dict_mapper)
    first = result_rdd.first()
    header = ["Zip_code, Vehicle_count_2012_2013"]
    if download:
        def add_header(records):
            for record in records:
                if record == first:
                    yield header
                    yield record
                else:
                    yield record

        def to_csv_line(data):
            return ','.join(str(d) for d in data)


        result_rdd.mapPartitions(add_header).map(to_csv_line).coalesce(1).saveAsTextFile(output_path + "/Raw Results/traffic_volume")

    return result_rdd
