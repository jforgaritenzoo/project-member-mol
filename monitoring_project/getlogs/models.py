from django.db import models


# Create your models here.
class TaskResult(models.Model):
    task_id = models.CharField(max_length=255)
    result_data = models.TextField()  # Store JSON or plain text
    timestamp = models.DateTimeField(auto_now_add=True)

class ConsumedData(models.Model):
    queue_name = models.CharField(max_length=255)
    payload = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.queue_name}: {self.payload}"
