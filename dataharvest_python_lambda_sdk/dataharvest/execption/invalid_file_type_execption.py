class InvalidFileTypeError(Exception):
    def __init__(self, file_type):
        self.file_type = file_type
        super().__init__(f"Invalid file type: {file_type}")