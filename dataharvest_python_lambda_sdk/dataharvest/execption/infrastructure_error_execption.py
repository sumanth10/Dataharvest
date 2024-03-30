class InfrastructureError(Exception):
    def __init__(self, output_type, message="Error when doing operation on cloud resources."):
        self.connection_type = output_type
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message} Output Type: {self.connection_type}"