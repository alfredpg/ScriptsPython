import geocoder
import csv
import json
geojson = {
    'type': 'FeatureCollection',
    'features': []
}

report_noloc = ''

results = 0
total = 0

with open('csv_in.csv', encoding="utf8") as csv_file:
    csv_data = csv.reader(csv_file, delimiter=',', quotechar='\'')
    next(csv_data) # skip header

    for row in csv_data:
        geodir = geocoder.google('{}, Córdoba, Andalucía, España'.format(row[1]))
        total += 1
        if geodir:
            print('{},{},{},{},{}'.format(
                int(row[0]), row[1], row[2], geodir.latlng[1], geodir.latlng[0]))
            geojson['features'].append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [geodir.latlng[1], geodir.latlng[0]],
                },
                "properties": {
                    "id": row[0],
                    "direccion": row[1],
                    "tipo": row[2],
                }
            })
            results += 1
        else:
            print('{},{},{},0,0'.format(int(row[0]), row[1], row[2]))
            report_noloc += '{},{},{}\n'.format(int(row[0]), row[1], row[2])

    print('Number of matches {}/{}'.format(results, total))

with open('geo_results_google.geojson', 'w') as geofile:
    geofile.write(json.dumps(geojson, indent=2))

with open('report_google.txt', 'w') as report_file:
    report_file.write('Number of matches {}/{}\n\n'.format(results, total))
    report_file.write(report_noloc)
