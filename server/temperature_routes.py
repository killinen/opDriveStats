"""Protected temperature dashboards, plotting data, and complete CSV exports."""

from __future__ import annotations

import csv
import gzip
import io
import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from . import temperature_data

router = APIRouter()
templates = Jinja2Templates(
    directory=str(Path(__file__).resolve().parent / 'templates')
)


def _response(request: Request, data: dict) -> Response:
    raw = json.dumps(data, separators=(',', ':'), allow_nan=False).encode()
    headers = {'Vary': 'Accept-Encoding'}
    accepts = False
    for part in request.headers.get('accept-encoding', '').lower().split(','):
        pieces = [piece.strip() for piece in part.split(';')]
        if pieces[0] == 'gzip':
            try:
                accepts = (
                    next(
                        (
                            float(piece[2:])
                            for piece in pieces[1:]
                            if piece.startswith('q=')
                        ),
                        1.0,
                    )
                    > 0
                )
            except ValueError:
                accepts = False
            break
    if accepts:
        raw = gzip.compress(raw, compresslevel=6, mtime=0)
        headers['Content-Encoding'] = 'gzip'
    return Response(raw, media_type='application/json', headers=headers)


def _report(device, profile, engine, phase):
    try:
        return temperature_data.repository.report(device, profile, engine, phase)
    except ValueError as error:
        raise HTTPException(422, str(error)) from error


@router.get('/temperatures', response_class=HTMLResponse)
def temperature_view(request: Request):
    return templates.TemplateResponse('temperatures.html', {'request': request})


@router.get('/api/temperatures', response_model=None)
def temperature_report(
    request: Request,
    device: str | None = None,
    profile: int | None = Query(default=None, ge=1),
    engine: str = 'All',
    phase: str = 'All',
):
    report = _report(device, profile, engine, phase)
    return _response(request, {**report, 'sync': temperature_data.repository.state()})


@router.get('/api/temperatures/drive', response_model=None)
def temperature_drive(
    request: Request,
    device: str,
    drive: str,
    profile: int = Query(default=2, ge=1),
):
    result = temperature_data.repository.drive(device, drive, profile)
    if result is None:
        raise HTTPException(404, 'Temperature drive not found')
    origin = result['metadata']['display_origin_ns']
    # Plotting seconds are convenient for browsers; CSV preserves integer timestamps.
    samples = [
        {
            **row,
            'time_s': (row['time_ns'] - origin) / 1e9,
            'time_ns': str(row['time_ns']),
        }
        for row in result['samples']
    ]
    return _response(request, {**result, 'samples': samples})


def _csv_rows(fields, rows):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    yield buffer.getvalue()
    for row in rows:
        buffer.seek(0)
        buffer.truncate(0)
        writer.writerow(row)
        yield buffer.getvalue()


@router.get('/api/temperatures/conditions.csv')
def conditions_csv(
    device: str | None = None,
    profile: int | None = Query(default=None, ge=1),
    engine: str = 'All',
    phase: str = 'All',
):
    report = _report(device, profile, engine, phase)
    fields = [
        'profile',
        'device',
        'engine_filter',
        'phase_filter',
        'ambient_confidence',
        'ambient_low',
        'ambient_high',
        'speed_band',
        'seconds',
        'drives',
        'min',
        'median',
        'p95',
        'max',
        'sustained',
        'delta_median',
        'delta_p95',
        'delta_seconds',
    ]
    rows = (
        {
            **cell,
            'profile': report['profile'],
            'device': device or 'All',
            'engine_filter': engine,
            'phase_filter': phase,
            'ambient_confidence': 'provisional',
            'ambient_high': cell['ambient_low'] + 5,
            'speed_band': temperature_data.SPEED_BANDS[cell['speed_band']],
        }
        for cell in report['cells']
    )
    return StreamingResponse(
        _csv_rows(fields, rows),
        media_type='text/csv',
        headers={
            'Content-Disposition': 'attachment; filename="temperature-conditions.csv"'
        },
    )


@router.get('/api/temperatures/samples.csv')
def samples_csv(device: str, drive: str, profile: int = Query(default=2, ge=1)):
    result = temperature_data.repository.drive(device, drive, profile)
    if result is None:
        raise HTTPException(404, 'Temperature drive not found')
    fields = [
        'device',
        'drive',
        'profile',
        'segment',
        'time_ns',
        'time_s',
        'metric',
        'value',
        'raw',
        'counter',
        'bus',
        'source_segment_index',
    ]
    origin = result['metadata']['display_origin_ns']
    rows = (
        {
            **sample,
            'device': device,
            'drive': drive,
            'profile': profile,
            'time_s': (sample['time_ns'] - origin) / 1e9,
        }
        for sample in result['samples']
    )
    return StreamingResponse(
        _csv_rows(fields, rows),
        media_type='text/csv',
        headers={
            'Content-Disposition': 'attachment; filename="temperature-samples.csv"'
        },
    )
