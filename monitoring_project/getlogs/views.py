from django.shortcuts import render
from .utils import fetch_data_from_api

def table_view(request):
    data = fetch_data_from_api()
    return render(request, 'table.html', {'data': data})

# Create your views here.
