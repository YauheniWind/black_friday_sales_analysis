def split_file(file_name, lines_per_file):
    with open(file_name, "r") as f:
        header = f.readline()

        file_num = 1
        line_count = 0
        out = open(f"small_file_{file_num}.csv", "w")
        out.write(header)

        for line in f:
            if line_count >= lines_per_file:
                out.close()
                file_num += 1
                line_count = 0

                out = open(f"small_file_{file_num}.csv", "w")
                out.write(header)

            out.write(line)
            line_count += 1

        out.close()

split_file("retail_black_friday_sales_100k.csv", 10000)