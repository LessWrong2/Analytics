from postgres_ops import get_pg_engine
from utils import timed
from pipeline_commands import pipeline_commands
import logging
import sys
logging.basicConfig(filename='db_sql.log', level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

@timed
def attempt_sql_command(conn, command, name):
    # try:
        logging.debug('executing {}!'.format(name))
        conn.execute(command)
    # except:
    #     logging.debug('command {} failed!'.format(command))


def execute_sql_commands(array_of_commands, conn=None):

    def run_commands(array_of_command_names, conn):
        [attempt_sql_command(conn, pipeline_commands[command_name], command_name) for command_name in array_of_command_names]

    if not conn:
        engine = get_pg_engine()
        with engine.begin() as conn:
            run_commands(array_of_commands, conn)
        engine.dispose()
    else:
        with conn.begin():
            run_commands(array_of_commands, conn)

def drop_tables():

    execute_sql_commands([
        'drop_user_day_post_views_table',
        'drop_core_events_cleaned_table',
        'drop_ssrs_cleaned_table',
    ])


def run_postgres_pipeline():
    # execute_sql_commands(['update_lessraw_small', 'update_lessraw_medium'])

    execute_sql_commands([
        'drop_ssrs_cleaned_table',
        'create_ssrs_cleaned_table',
        'drop_core_events_cleaned_table',
        'create_core_events_cleaned_table',
        'drop_user_day_post_views_table'
        'create_user_day_post_views_table'
    ])

