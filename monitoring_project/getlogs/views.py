from django.http import JsonResponse
from django.shortcuts import render
from .utils import fetch_api_logs
from django.shortcuts import render
from .models import TaskResult, ConsumedData
from django.views.decorators.csrf import csrf_exempt  # Only if CSRF token isn't used
from .tasks import consume_from_queue

# Create your views here.

@csrf_exempt  # Use this if you're testing with POST requests and no CSRF token
def start_consumer(request):
    if request.method == "POST":
        queue_name = request.POST.get('queue_name')  # Pass the queue name dynamically
        if not queue_name:
            return JsonResponse({"error": "Queue name is required"}, status=400)

        # Trigger Celery task
        consume_from_queue.delay(queue_name)
        return JsonResponse({"status": "Consumer started for queue: " + queue_name})

    return JsonResponse({"error": "Invalid request method"}, status=405)


def consumed_data_view(request):
    data = ConsumedData.objects.all().order_by("-created_at")
    return render(request, "consume.html", {"results": data})


# def consume(request):
#     # Fetch the latest task results
#     results = TaskResult.objects.order_by("-timestamp")  # Newest first
#     return render(request, "consume.html", {"results": results})


def index(request):
    return render(request, "index.html")


def table_view(request):
    data = fetch_api_logs()
    return render(request, "table.html", {"data": data})


def runjobs(request):
    return render(request, "runjobs.html")


# def consume(request):
#     return render(request, "consume.html")
