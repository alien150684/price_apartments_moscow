from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    import os
    
    hook = TelegramHook(token=os.environ.get('TELEGRAM_TOKEN'), 
                        chat_id=os.environ.get('TELEGRAM_CHAT_ID'))
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
            'chat_id': os.environ.get('TELEGRAM_CHAT_ID'),
            'text': message
        }) # отправление сообщения

def send_telegram_failure_message(context): # на вход принимаем словарь со контекстными переменными
    import os
    
    hook = TelegramHook(token=os.environ.get('TELEGRAM_TOKEN'), 
                        chat_id=os.environ.get('TELEGRAM_CHAT_ID'))
    # dag = context['dag'].dag_id
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = f'Ошибка DAG: {task_instance_key_str} с id={run_id}' # определение текста сообщения
    hook.send_message({
            'chat_id': os.environ.get('TELEGRAM_CHAT_ID'),
            'text': message
        }) # отправление сообщения