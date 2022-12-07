import random
from sqlalchemy import create_engine, Table, Column, \
    String, DateTime, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlfuzz.common import ret_typename_from_class, load_json
from sqlfuzz.model import Scope, QuerySpec
from sqlalchemy import select, asc
from statistics import mean
from sqlfuzz.randoms import *


class TableStat(object):
    # maintain statistics for each table
    def __init__(self, tablename):
        self.tablename = tablename
        self.columns = []
        self.column_name = []
        self.column_type = []

        # min, max, average
        self.columns_stat = []
        self.table_size = 0

        # sqlalchemy table
        # self.sqlalchemy_tbl = None

    """
    def add_sqlalchemy_tbl(self, tbl):
        self.sqlalchemy_tbl = tbl
    """

    def add_column(self, column_name, column_type):
        self.column_name.append(column_name)
        self.column_type.append(column_type)
        self.columns.append([])

    # get row-wise data and transform to column-wise data
    def add_data(self, data):
        for x in range(len(data)):
            self.columns[x].append(data[x])
            self.table_size += 1

    # ret stat data by columnname
    def ret_stat(self, columnname):
        for x in range(len(self.column_name)):
            if self.column_name[x] == columnname:
                return self.columns_stat[x]
            else:
                AssertionError("No matching column name, my mistake")

    # ret string data by columnname
    def ret_string(self, columnname):
        for x in range(len(self.column_name)):
            if self.column_name[x] == columnname:
                return self.columns[x]
            else:
                AssertionError("No matching column name, my mistake")

    @staticmethod
    def ret_table_with_tblname(sqlalchemy_tbllist, tblname):
        for idx in range(len(sqlalchemy_tbllist)):
            name = sqlalchemy_tbllist[idx].name
            # print(name)
            if tblname == name:
                return sqlalchemy_tbllist[idx]
        return None

    @staticmethod
    def ret_tablestat_with_tblname(tbl_stat_list, tblname):
        for idx in range(len(tbl_stat_list)):
            name = tbl_stat_list[idx].tablename
            if tblname == name:
                return tbl_stat_list[idx]
        return None

    # when insertion is done, we calculate the stat

    def calculate_stat(self):

        # debug
        # print(self.columns)

        for x in range(len(self.columns)):

            # 1) if string/text ==> store length
            if self.column_type[x] == "String":
                temp_arr = []
                for y in range(len(self.columns[x])):
                    temp_arr.append(len(self.columns[x][y]))

                _min, _max, _avg = self.stat_from_arr(temp_arr)

            # 2) if DateTime
            elif self.column_type[x] == "DateTime":
                temp_arr = []
                for y in range(len(self.columns[x])):
                    # print("sampled datatime", y)
                    temp_arr.append(
                        int(self.columns[x][y].strftime("%Y%m%d %H:%M:%S")))
                    # temp_arr.append(int(self.columns[x][y]))

                _min, _max, _avg = self.stat_from_arr(temp_arr)

            # 3) if numetic
            else:
                _min, _max, _avg = self.stat_from_arr(self.columns[x])

            self.columns_stat.append([_min, _max, _avg])

    def calculate_stat_existing_db(self, column_data, x):
        # call once for each column, different from previous method calculate_stat and populate data
        # debug
        # print(self.columns)
        # print("column data is ",column_data, x, self.column_type[x], type(column_data[0]))
        # 1) if string/text ==> store length

        if self.column_type[x] == "String":
            temp_arr = []
            for y in range(len(column_data)):
                if not column_data[y]:
                    temp_arr.append(0)
                else:
                    temp_arr.append(len(column_data[y]))

            _min, _max, _avg = self.stat_from_arr(temp_arr)
        elif isinstance((column_data[0]), str) or self.column_type[x] == "Array":
            # get stat for a char(1) column
            # print("Char(1)")
            temp_arr = []
            for y in range(len(column_data)):
                temp_arr.append(len(column_data[y]))
            _min, _max, _avg = self.stat_from_arr(temp_arr)

        # 2) if DateTime
        elif isinstance((column_data[0]), datetime.date):
            temp_arr = []
            for y in range(len(column_data)):
                temp_arr.append(int(column_data[y].strftime("%Y%m%d")))
                # print("sampled datatime", column_data[y])
                # temp_arr.append(int(column_data[y]))

            _min, _max, _avg = self.stat_from_arr(temp_arr)
        elif self.column_type[x] == "ByteA":
            return
        # 3) if numetic
        else:
            _min, _max, _avg = self.stat_from_arr(column_data)

        self.columns_stat.append([_min, _max, _avg])
        self.columns[x].extend(column_data)
        # print("finish run update for this column")

    def stat_from_arr(self, array):
        _min = min(array)
        _max = max(array)
        _avg = mean(array)
        return _min, _max, _avg


class TableSpec(object):
    def __init__(self, name):
        self.table_name = name
        self.columns = []
        self.row_data = []
        self.pk_idx = None
        self.fk_idx = -1
        self.num_tuples = -1

    def add_column(self, column_name, column_type):
        self.columns.append((column_name, column_type))


class Fuzz:
    def __init__(self, prob_config, connstr):
        self.prob_config = prob_config
        self.connstr = connstr
        self.__load_existing_dbschema()

    def __tables(self, engine):
        q = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
            """
        tablenames = []
        results = engine.execute(q).fetchall()
        for tablename in results:
            tablenames.append(tablename[0])
        return tablenames

    def __load_existing_dbschema(self):
        tables = []  # tables spec (Tableclass), name,
        tables_stat = []  # tables_stat # tables statistics (TableStat class)
        postgres_engine = create_engine(self.connstr)
        table_names = self.__tables(postgres_engine)
        schemameta = MetaData(postgres_engine)
        DBSession = sessionmaker(bind=postgres_engine)
        session = DBSession()
        sqlalchemy_tables = []
        for table_name in table_names:
            messages = Table(table_name,
                             schemameta,
                             autoload=True,
                             autoload_with=postgres_engine)
            sqlalchemy_tables.append(messages)

            table_stat = TableStat(table_name)
            table_class = TableSpec(table_name)

            table_class.pk_idx = -1
            table_class.fk_idx = -1
            results = session.query(messages)
            sample_results = (results[:5])
            column_index = 0

            for c in messages.columns:
                column_data = [i[column_index] for i in sample_results]
                # need to use sqlalchemy type instead of real database's type
                table_class.add_column(c.name, (c.type))
                typename = ret_typename_from_class(c.type)
                table_stat.add_column(c.name, typename)
                # some type may not use for intersection calculation
                column_index += 1

            tables.append(table_class)
            tables_stat.append(table_stat)
            for c in range(len(messages.columns)):
                column_data = [i[c] for i in sample_results]
                # print(sample_results[messages.columns.index(c)])
                tables_stat[(
                    tables_stat).index(table_stat)].calculate_stat_existing_db(
                        column_data, c)
            # update stat for each table
            table_stat.table_size = len(results.all())

        self.tables = tables
        self.tables_stat = tables_stat
        self.sqlalchemy_tables = sqlalchemy_tables

    def __stmt_complex(self, stmt, available_columns):
        if (random_int_range(1000) < self.prob_config["group"]):
            chosen_groupby_columns = random.choices(
                available_columns,
                k=random_int_range(len(available_columns)))
            for column in chosen_groupby_columns:
                stmt = stmt.group_by(column)
        # distinct entire select
        if (random_int_range(1000) < self.prob_config["distinct"]):
            stmt = stmt.distinct()
        # order
        if (random_int_range(1000) < self.prob_config["order"]):
            chosen_orderby_columns = random.choices(available_columns, k=1)
            for column in chosen_orderby_columns:
                if (ret_typename_from_class(column.type) in ["Float", "Integer"]):
                    stmt = stmt.order_by(asc(column))
        # limit
        if (random_int_range(1000) < self.prob_config["limit"]):
            stmt = stmt.limit(random_int_range_contain_zero(20))

        if (random_int_range(1000) < self.prob_config["offset"]):
            stmt = stmt.offset(random_int_range_contain_zero(20))
        return stmt

    def gen(self, spec):
        select_columns, where_clause, table_idx, selectable_columns, joined_from, base_table = spec.gen_select_statement(
            fuzzer=self)
        if joined_from is not None:
            stmt = select(select_columns).select_from(
                joined_from)
        else:
            stmt = select(select_columns)
        if where_clause is not None:
            stmt = stmt.where(where_clause)
        stmt = self.__stmt_complex(stmt, selectable_columns)
        return stmt

    def gen_orm_queries(self, count=20):
        scope = Scope()
        scope.add_alc(self.sqlalchemy_tables)
        spec = QuerySpec("demo", self.tables, self.tables_stat, scope)
        queries = []
        limit = count * 5
        counter = 0
        pg_engine = create_engine(self.connstr)
        while count:
            if counter > limit:
                break
            counter += 1
            stmt = self.gen(spec)
            try:
                pg_engine.execute(stmt)
                queries.append(stmt)
                spec.subqueries.append(stmt)
                count -= 1
            except:
                # print(counter)
                continue

        return queries
