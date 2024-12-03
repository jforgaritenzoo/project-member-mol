from django.test import TestCase
from tasks import add

# Trigger the task
task = add.delay(4, 5)

# Wait for and retrieve the result
result = task.get(timeout=10)
print(f"Task result: {result}")
