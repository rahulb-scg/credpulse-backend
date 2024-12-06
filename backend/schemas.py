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
    
# Example schema for any JSON payload
class DataSchema(Schema):
    key = fields.String(required=True)
    value = fields.Integer(required=True)

# Example validation error handler
def handle_validation_error(error):
    return {"errors": error.messages}, 400
