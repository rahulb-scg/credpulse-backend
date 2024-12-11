from marshmallow import Schema, fields, ValidationError

# File upload schema
class FileUploadSchema(Schema):
    files = fields.List(
        fields.Raw(required=True), 
        required=True, 
        error_messages={"required": "Files are required."}
    )

# File download schema
class FileDownloadSchema(Schema):
    filename = fields.String(required=True, validate=lambda x: len(x) > 0, error_messages={
        "required": "Filename is required.",
        "validator_failed": "Filename must not be empty."
    })
    
class NewReportSchema(Schema):
    report_name = fields.String(required=True, validate=lambda x: len(x) > 0, error_messages={
        "required": "Report name is required.",
        "validator_failed": "Report name must not be empty."
    })
    description = fields.String(required=False)
    data_file = fields.Raw(required=True, error_messages={"required": "Data file is required."})
    config_file = fields.Raw(required=True, error_messages={"required": "Config file is required."})


# Example validation error handler
def handle_validation_error(error):
    return {"errors": error.messages}, 400
