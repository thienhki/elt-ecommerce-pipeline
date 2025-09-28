import pandas as pd

class TemplateOperatorDB:
    def __init__(self, table_name):
        self.table_name = table_name

    def create_query_insert_into(self, dataframe):
        col = ", ".join(dataframe.columns)
        val = ", ".join(["%s"] * len(dataframe.columns))
        odku = ", ".join([f"{c} = VALUES({c})" for c in dataframe.columns])
        create_query = (
            f"INSERT INTO {self.table_name} ({col}) " 
            f"VALUES ({val}) "
            f"ON DUPLICATE KEY UPDATE {odku};"
        )

        return create_query
    
    def delete_query_where_in(self, column_name, list_values):
        val = ", ".join(["%s"] * len(list_values))
        create_query = (    
            f"DELETE FROM {self.table_name} "
            f"WHERE {column_name} IN ({val});"
        )
        return create_query
                    

    # data =  pd.DataFrame({
    #     "id": [1, 2, 3],
    #     "name": ["Alice", "Bob", "Charlie"],
    #     "age": [25, 30, 35]
    # })

    # print(data)
    # template = TemplateOperatorDB("users")
    # print(template.create_query_insert_into(data))
    # print(template.delete_query_where_in("id", [1, 2]))
    # # DELETE FROM users
    # # WHERE id IN (%s, %s)

    # print(["s"] * 4)