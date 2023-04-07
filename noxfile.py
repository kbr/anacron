import nox


PYTHON_TEST_VERSIONS = ("3.7", "3.8", "3.9", "3.10", "3.11", "3.12")


@nox.session(python=PYTHON_TEST_VERSIONS)
def test(session):
    session.install("-e", ".")
    session.run("python", "-m", "unittest")


@nox.session
def pylint(session):
    session.install("-e", ".")
    session.install("pylint")
    session.run("pylint", "anacron")
