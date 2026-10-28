from utilities import send_whatsapp


def send_alert_to_driver(alert_msg):
    '''
    :param alert_msg: sample payload: {'vehicle_id': 'VEH_007', 'timestamp': '2025-12-09T17:36:43.871308+00:00', 'alert_type': 'SPEEDING', 'speed_kmph': 112, 'fuel_percent': 21, 'temperature_c': 84, 'latitude': 40.469962, 'longitude': -73.359513} 
    :return: 
    '''
    vehicle_id_to_driver_number_map = {
        'VEH_007': '+9188888',
        # 'VEH_001': '+91333333',
        # 'VEH_002': '+91444444'
    }
    
    number_to_send_alert_to = vehicle_id_to_driver_number_map.get(alert_msg['vehicle_id'], 0)
    
    if number_to_send_alert_to != 0:
        message = f"""⚠️ ALERT: {alert_msg['alert_type']} ⚠️
        VEHICLE ID: {alert_msg['vehicle_id']}
        SPEED: {alert_msg['speed_kmph']} KM/HR
        FUEL: {alert_msg['fuel_percent']}%
        TEMPERATURE: {alert_msg['temperature_c']}°C
        LOCATION: {alert_msg['latitude']}, {alert_msg['longitude']}
        TIMESTAMP: {alert_msg['timestamp']}"""

        send_whatsapp(number_to_send_alert_to, message)
        print(f"Alert sent to driver for vehicle ID: {alert_msg['vehicle_id']}")
    else:
        print("No driver number found for vehicle ID:")

if __name__ == '__main__':
    send_alert_to_driver({
        'vehicle_id': 'VEH_007',
        'timestamp': '2025-12-09T17:36:43.871308+00:00',
        'alert_type': 'SPEEDING',
        'speed_kmph': 112,
        'fuel_percent': 21,
        'temperature_c': 84,
        'latitude': 40.469962,
        'longitude': -73.359513
    })
