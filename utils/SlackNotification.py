from airflow.providers.slack.operators.slack import SlackAPIPostOperator
import pendulum
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")



# Slack token and channel configuration

SLACK_CONN_ID = 'slack_default'  # Conn ID của Slack mà bạn đã tạo
SLACK_CHANNEL = '#aiflow'  # Thay bằng tên kênh của bạn
SLACK_USER_IDS = ['U07FXBVCR9R']  # Danh sách các user_id của các thành viên cần mention
    
def send_slack_success_message(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date=str(local_tz.convert(context.get('execution_date'))),
    log_url = task_instance.log_url

    # Tạo chuỗi mention các thành viên
    mentions = ' '.join([f'<@{user_id}>' for user_id in SLACK_USER_IDS])

    success_alert = SlackAPIPostOperator(
        task_id='slack_success',
        slack_conn_id=SLACK_CONN_ID,
        channel=SLACK_CHANNEL,
        text=f"""
            :white_check_mark: Task Succeeded.
            *Dag*: {dag_id}
            *Task*: {task_id}
            *Execution Time*: {execution_date}
            *Log Url*: {log_url}
            {mentions} Great job!
        """,
    )
    return success_alert.execute(context=context)

def send_slack_failure_message(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date=str(local_tz.convert(context.get('execution_date'))),
    log_url = task_instance.log_url
    exception = context.get('exception')

    # Tạo chuỗi mention các thành viên
    mentions = ' '.join([f'<@{user_id}>' for user_id in SLACK_USER_IDS])
    
    error_alert = SlackAPIPostOperator(
        task_id='slack_failure',
        slack_conn_id=SLACK_CONN_ID,
        channel=SLACK_CHANNEL,
        text=f"""
            :x: Task Failed.
            *Dag*: {dag_id}
            *Task*: {task_id}
            *Execution Time*: {execution_date}
            *Log Url*: {log_url}
            *Exception*: {exception},
            {mentions} Please check this!
        """,
    )

    return error_alert.execute(context=context)
