from job_broker.tasks.hello.tasks import hello


def test_0010_task_hello():
    r = hello(1, 2, 100)
    assert r == [1, 2, 100]


