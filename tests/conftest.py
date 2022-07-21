class MockCursor:
    def execute(self):
        pass


class MockSnowflakeConnector:

    database: str
    cursor: object

    def connect(self):
        pass
