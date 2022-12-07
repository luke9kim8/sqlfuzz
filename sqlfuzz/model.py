import random
from sqlfuzz.wheregen import *
from sqlfuzz.randoms import random_int_range
from sqlfuzz.common import get_selectable_column
from sqlfuzz.wheregen import where_generator
from sqlalchemy import *


class Scope(object):
    # to model column/table reference visibility
    # store a list of subquery named as "d_int"
    # when the size exceeds a given limit, randomly remove one
    table_ref_list = []
    # this is for scalar subquery construction
    table_ref_stmt_list = []

    def __init__(self,
                 columnlist=None,
                 tablelist=None,
                 parent=None,
                 table_spec=None):
        # available for column and table ref
        if (columnlist is not None):
            self.column_list = columnlist
        else:
            self.column_list = []
        if (tablelist is not None):
            self.table_list = tablelist
        else:
            self.table_list = []
        # self.spec = table_spec
        if parent is not None:
            # self.spec = parent.spec
            self.column_list = parent.column_list
            self.table_list = parent.table_list
            self.stmt_seq = parent.stmt_seq
        self.alc_tables = None
        # Counters for prefixed stmt-unique identifiers
        # shared_ptr<map<string,unsigned int> >
        # "ref_" to an index
        self.stmt_seq = {}
        # print("running constructor for scope")

    def add_alc(self, alc_tables):
        self.alc_tables = alc_tables


class Prod(object):
    def __init__(self, name, spec, spec_stat, scope, parent=None):
        self.pprod = parent
        self.name = name
        # record table's column and its type
        self.spec = spec
        # record tables' simple stat
        self.spec_stat = spec_stat
        # model column/table reference visibility
        self.scope = scope


class From_Clause(Prod):
    # static variable for the class, store a list of subquery from previous runs
    def __init__(self, name, spec, spec_stat, scope, subqueries, parent=None, prob_conf=None):
        # # print("running constructor for from clause")
        super().__init__(name, spec, spec_stat, scope, parent)
        self.prob_conf = prob_conf
        self.subqueries = subqueries

    def get_random_from_clause(self, fuzzer=None, query_spec=None):
        # branch choice either simple table, subquery, or new joined stmt
        randval = random_int_range(100)
        if (randval > 95):
            return random.choice(self.scope.alc_tables + self.subqueries), None, None
        if (randval > 70) and len(self.subqueries) > 0:
            return random.choice(self.subqueries), None, None
        elif (randval > 30):
            stmt = fuzzer.gen(query_spec)
            return stmt, None, None
        else:
            return self.get_joined_table()

    def get_joined_table(self):
        # step 1. get two table_ref
        # step 2. perform join operation on two table_ref
        table_a = random.choice(self.scope.alc_tables)
        table_b = random.choice(self.scope.alc_tables)
        while (table_a == table_b):
            table_b = random.choice(self.scope.alc_tables)
        random_column = random.choice(get_selectable_column(table_a))
        join_condition = None
        j = None
        for c in table_b.columns:
            if (isinstance(c.type, type(random_column.type))):
                srctype = str(c.type)
                if ("FLOAT" in srctype or "INT" in srctype) is False:
                    continue
                # generate join type
                join_type = random.choice(
                    ["true", "condition", "inner", "full", "left"])
                if (join_type == "true"):
                    join_condition = true()
                elif (join_type == "condition"):
                    join_condition = join_cond_generator(c, random_column)
                elif (join_type == "inner"):
                    join_condition = join_cond_generator(c, random_column)
                    j = table_a.join(table_b, join_condition)
                elif (join_type == "left"):
                    try:
                        j = table_a.join(table_b, join_condition, isouter=True)
                    except:
                        j = table_a.join(table_b, true(), isouter=True)
                else:
                    try:
                        j = table_a.outerjoin(table_b,
                                              join_condition,
                                              full=True)
                    except:
                        j = table_a.outerjoin(table_b, true(), full=True)
                break
        if (j is not None):
            # print("success join")
            return table_a, table_b, j
        else:
            return table_a, None, None


class Select_List(Prod):
    def __init__(self, name, spec, spec_stat, scope, parent=None):
        super().__init__(name, spec, spec_stat, scope, parent)
        self.value_exprs = []
        # columns is for subquery constrction, renaming purpose
        self.columns = 0
        # derived_table is for gen_select_statement and subquery construction
        self.derived_table = {}

    def gen_select_expr(self, from_clause, number_columns=None):
        selectable_columns = get_selectable_column(from_clause)
        selectable_columns_length = len(selectable_columns)
        if (number_columns is None):
            number_columns = random_int_range(selectable_columns_length)
        chosen_columns_obj = random.sample(selectable_columns, number_columns)
        out = chosen_columns_obj

        # make some columns have literal values
        for i in range(len(out)):
            if (random.randint(1, 10) > 9):
                new_type = (out[i].type)
                new_type_str = str(out[i].type)
                literal_column_obj = None
                if "CHAR" in new_type_str:
                    literal_string = "literal_column_str"
                    literal_column_obj = literal_column(literal_string,
                                                        type_=String)
                elif "FLOAT" in new_type_str or "INT" in new_type_str:
                    literal_column_obj = literal_column(
                        '1337', type_=Integer)
                if literal_column_obj is not None:
                    out[i] = type_coerce(literal_column_obj, new_type).label(
                        name="c_" + str(random.randint(1, 5000)))

        # functions to increase the variety of selectable objects
        for i in range(len(out)):
            # TODO: unhardcode this
            if (random_int_range(10) > 5):
                if (random_int_range(10) > 5):
                    new_type = out[i].type
                    out[i] = type_coerce((func.distinct(out[i])), new_type).label(
                        name="c_" + str(random.randint(1, 5000)))
                # second generate function
                selectable_func_list = get_compatible_function(out[i])
                selected_func = random.choice(selectable_func_list)
                new_type = out[i].type
                # only count function would change the type of the column
                if selected_func == func.count:
                    new_type = Float  # shouldn't this be int??
                if (random_int_range(10) > 5):
                    if (random_int_range(100) > 75):
                        out[i] = type_coerce(
                            (selected_func(out[i])).over(
                                partition_by=random.sample(
                                    selectable_columns, 1),
                                order_by=random.sample(selectable_columns, 1)),
                            new_type).label(name="c_" +
                                            str(random.randint(1, 5000)))
                    elif (random_int_range(100) > 50):
                        out[i] = type_coerce((selected_func(out[i])).over(
                            partition_by=random.sample(selectable_columns, 1)),
                            new_type).label(
                            name="c_" +
                            str(random.randint(1, 5000)))
                    elif (random_int_range(100) > 25):
                        out[i] = type_coerce(
                            (selected_func(out[i])).over(
                                order_by=random.sample(selectable_columns, 1)),
                            new_type).label(name="c_" +
                                            str(random.randint(1, 5000)))
                    else:
                        out[i] = type_coerce(
                            (selected_func(out[i])).over(),
                            new_type).label(name="c_" +
                                            str(random.randint(1, 5000)))
                else:
                    out[i] = type_coerce(
                        (selected_func(out[i])),
                        new_type).label(name="c_" +
                                        str(random.randint(1, 5000)))
        self.value_exprs = out
        return (self.value_exprs), selectable_columns


class QuerySpec(Prod):
    # top class for generating select statement
    def __init__(self, name, spec, spec_stat, scope, parent=None):
        super().__init__(name, spec, spec_stat, scope, parent)
        self.from_clause = []
        self.select_list = []
        self.limit_clause = None
        self.offset_clause = None
        self.scope = scope
        self.entity_list = []
        self.subqueries = []
        # print("running constructor for query_spec")

    def get_table_idx_from_column_name(self, column_name):
        # input: convoluted column name resulting from alias rename
        # output: table_idx and correspond simple columname
        suffix_column_name = column_name.split(".")[-1]
        # print("column_name is", suffix_column_name)
        for i in range(len(self.spec_stat)):
            t_spec = self.spec_stat[i]
            for c in t_spec.column_name:
                if c in suffix_column_name:
                    # print("table idx found", i)
                    return i, c
        return None, None

    def gen_select_statement(self,
                             fuzzer=None,
                             select_column_number=None,
                             force_simple_from=False):
        # parameter needed: prod, scope

        # 1. ########## generate from_clause ##########
        #     get a random table
        base_table = False
        # print("running simple constructor for select from")
        from_ins = From_Clause(self.name, self.spec, self.spec_stat,
                               self.scope, self.subqueries)
        # print("table_ref_list", Scope.table_ref_list)
        from_clause1, from_clause2, joined_from = from_ins.get_random_from_clause(
            fuzzer=fuzzer, query_spec=self)
        # print("from_clause is", from_clause1, from_clause2, joined_from)
        if ("Table" in str(type(from_clause1))):
            base_table = True
        # print(type(from_clause1), type(from_clause2))
        # ########## should decide where to select from by this point ##########
        # 2. generate select_expr
        select_list = Select_List(self.name, self.spec, self.spec_stat,
                                  self.scope)
        # TODO: the function call gen_select_expr should be like a loop that update derived table
        if (joined_from is not None):
            select_list_expr, selectable_columns_obj = select_list.gen_select_expr(
                joined_from, number_columns=select_column_number)
        else:
            select_list_expr, selectable_columns_obj = select_list.gen_select_expr(
                from_clause1, number_columns=select_column_number)

        # for join cases, only one joined item would affect the where clause
        where_clause = self.gen_where_clause(
            select_list_expr, selectable_columns_obj)
        selectable_columns = []
        if joined_from is not None:
            selectable_columns = get_selectable_column(
                from_clause1) + get_selectable_column(from_clause2)
        else:
            selectable_columns = get_selectable_column(from_clause1)
        return (select_list_expr
                ), where_clause, None, selectable_columns, joined_from, base_table

    def gen_where_clause(self, select_list_expr, selectable_columns_obj):
        # generate where clause according to the sqlalchemy column
        # 1) select a sql alchemy column
        CONJ = ["and", "or"]
        num_where = min(3, random_int_range(len(selectable_columns_obj)))
        where_clause_list = []
        for i in range(num_where):
            random_column_object = random.choice(selectable_columns_obj)
            # get the column object
            try:
                # handle joined table where column names do not always
                # belong to the same table
                columnname = str(random_column_object).split(".")[-1]
                table_idx, columnname = self.get_table_idx_from_column_name(
                    columnname)
                if table_idx is None:
                    # this is not an original column
                    column_where = where_generator(random_column_object, None,
                                                   None, None, None)
                else:
                    column_stat = self.spec_stat[table_idx].ret_stat(
                        columnname)
                    column_data = self.spec_stat[table_idx].ret_string(
                        columnname)
                    column_where = where_generator(random_column_object, None,
                                                   column_stat, None,
                                                   column_data)
                if column_where is not None:
                    where_clause_list.append(column_where)
            except Exception:
                # print("fail in gen where")
                continue

            random_idx_list = list(range(len(Scope.table_ref_stmt_list)))
            random.shuffle(random_idx_list)

            # choose from an existing stmt
            for idx in random_idx_list:
                s = Scope.table_ref_stmt_list[idx]
                if len(s.columns) == 1:
                    srctype = str(get_selectable_column(s)[0].type)
                    column_stat = None
                    column_data = None
                    # print("orig scalar subquery type", srctype)
                    if "CHAR" in srctype:
                        column_data = [conf.SCALAR_STR]
                    elif "FLOAT" in srctype or "INT" in srctype or "NUMERIC" in srctype:
                        column_data = [int(conf.SCALAR_INT)]
                        column_stat = [int(conf.SCALAR_INT)]
                    else:
                        continue
                    scalar_stmt = s.limit(1).as_scalar()
                    column_where = where_generator(scalar_stmt, None,
                                                   column_stat, None,
                                                   column_data)
                    if column_where is not None:
                        where_clause_list.append(column_where)
                        break

        # begin merging the where clause
        parenthesis = False
        while (len(where_clause_list) > 1):
            where1 = where_clause_list[0]
            if where1 is None:
                where_clause_list.remove(where1)
                continue
            where2 = where_clause_list[1]
            if where2 is None:
                where_clause_list.remove(where2)
                continue
            combined_where = None
            if parenthesis is False:
                combined_where = combine_condition(where1, where2,
                                                   random.choice(CONJ))
                parenthesis = True
            else:
                combined_where = combine_parenthesis(where1, where2,
                                                     random.choice(CONJ))
            where_clause_list.remove(where1)
            where_clause_list.remove(where2)
            where_clause_list.insert(0, combined_where)

        if len(where_clause_list) > 0:
            return where_clause_list[0]
        else:
            return None
