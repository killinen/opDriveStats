"""Protected TRQI dashboard and lossless report exports."""

import csv
import io
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates

from ..temperature_routes import _response
from . import data

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / 'templates'))


def report(device=None, firmware=None, phase='All', hvac='All'):
    try:
        return data.repository.report(device, firmware, phase, hvac)
    except ValueError as error:
        raise HTTPException(422, str(error)) from error


@router.get('/trqi-temperatures')
def dashboard(request: Request):
    return templates.TemplateResponse('trqi.html', {'request': request})


@router.get('/api/trqi-temperatures', response_model=None)
def conditions(
    request: Request,
    device: str | None = None,
    firmware: str | None = None,
    phase: str = 'All',
    hvac: str = 'All',
):
    return _response(request, report(device, firmware, phase, hvac))


def drive_data(device, drive):
    result = data.repository.drive(device, drive)
    if result is None:
        raise HTTPException(404, 'TRQI drive not found')
    return result


@router.get('/api/trqi-temperatures/drive', response_model=None)
def drive_report(request: Request, device: str, drive: str):
    result = drive_data(device, drive)
    origin = result['metadata']['display_origin_ns']
    for key in ('grid', 'samples'):
        result[key] = [
            {
                **row,
                'time_s': (row['time_ns'] - origin) / data.SECOND,
                'time_ns': str(row['time_ns']),
            }
            for row in result[key]
        ]
    return _response(request, result)


def csv_rows(fields, rows):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    yield buffer.getvalue()
    for row in rows:
        buffer.seek(0)
        buffer.truncate()
        writer.writerow(row)
        yield buffer.getvalue()


@router.get('/api/trqi-temperatures/conditions.csv')
def conditions_csv(
    device: str | None = None,
    firmware: str | None = None,
    phase: str = 'All',
    hvac: str = 'All',
):
    rows = report(device, firmware, phase, hvac)['cells']
    fields = (
        'kind',
        'band',
        'phase',
        'seconds',
        'drives',
        'median',
        'p95',
        'maximum',
        'seconds_ge_82',
        'seconds_ge_85',
    )
    return StreamingResponse(
        csv_rows(fields, rows),
        media_type='text/csv',
        headers={'Content-Disposition': 'attachment; filename="trqi-conditions.csv"'},
    )


@router.get('/api/trqi-temperatures/samples.csv')
def samples_csv(device: str, drive: str):
    rows = drive_data(device, drive)['samples']
    fields = (
        'time_ns',
        'time_s',
        'segment',
        'metric',
        'value',
        'raw',
        'counter',
        'status',
        'loop_max_ms',
        'crc_valid',
        'sensor_valid',
        'frame_hex',
    )
    return StreamingResponse(
        csv_rows(fields, rows),
        media_type='text/csv',
        headers={'Content-Disposition': 'attachment; filename="trqi-samples.csv"'},
    )
