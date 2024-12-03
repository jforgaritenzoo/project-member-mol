from django.contrib import admin
from .models import TaskResult, ConsumedData
# Register your models here.

@admin.register(TaskResult)
class TaskResultAdmin(admin.ModelAdmin):
    list_display = ('task_id', 'timestamp')
    
@admin.register(ConsumedData)
class ConsumedDataAdmin(admin.ModelAdmin):
    list_display = ('queue_name', 'payload', 'created_at')