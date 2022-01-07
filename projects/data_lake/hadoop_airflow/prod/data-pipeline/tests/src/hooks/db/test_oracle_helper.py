from hooks.db.oracle_helper import OracleHelper


def test_oracle_conn_db_transfers():
    """
    Tests whether the connection in Airflow has been correctly created
    and validates that the connection is open
    """
    assert 1 == OracleHelper('DB_trans') \
        .get_rows('SELECT 1 db_name.table_name FETCH FIRST 1 ROWS ONLY')


if __name__ == '__main__':
    """pytest tests/src/hooks/db/test_oracle_helper.py -v"""
    test_oracle_conn_db_transfers()

    # TODO
    # conn: correctly created
    # conn: conn in open
    # conn: limit connection
    # conn: item by page
    # unit_test: return type
    # unit_test: parameters types (no)
    # mock
