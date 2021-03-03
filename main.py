from database.pg import POSTGERS


def main():
    pg = POSTGERS()
    table_list = pg.get_all_tables()
    column_type_list = []
    for t in table_list:
        res = pg.get_table_structures(t)
        if res is not None:
            column_type_list.extend(pg.get_table_structures(t))
    print(list(set(column_type_list)))


if __name__ == "__main__":
    main()
