from commons.helper import read_csv, send_request
from commons.logger import LOGGER

URL = 'http://127.0.0.1:5000'

vals = {
    'jobs' : 'files/jobs.csv',
    'departments' : 'files/departments.csv',
    'employees' : 'files/hired_employees.csv',
}

def jobs():
    LOGGER.info('execution started for jobs!!')
    batch = read_csv(vals['jobs'], ['id', 'job'])

    response = send_request(f'{URL}/jobs', batch)
    LOGGER.info(f'Response: {response}')

def departments():
    LOGGER.info('execution started for departments!!')
    batch = read_csv(vals['departments'], ['id', 'department'])

    response = send_request(f'{URL}/departments', batch)
    LOGGER.info(f'Response: {response}')

def employees():
    LOGGER.info('execution started for employees!!')
    batch = read_csv(vals['employees'], ['id', 'name', 'datetime', 'department_id', 'job_id'])

    response = send_request(f'{URL}/employees', batch)
    LOGGER.info(f'Response: {response}')


if __name__ == "__main__":
    jobs()
    departments()
    employees()
