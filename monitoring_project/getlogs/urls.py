from django.urls import path
from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("table", views.table_view, name="table"),
    path("runjobs", views.runjobs, name="runjobs"),
    path("consume", views.consumed_data_view, name="consume"),
    path("start-consumer/", views.start_consumer, name="start_consumer"),
    # path("task-results", views.task_results_view, name="task_results"),
]
