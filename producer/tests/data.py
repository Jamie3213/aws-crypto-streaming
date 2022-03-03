error = {
    "messageType": "E",
    "response": {"message": "Authorization failed", "code": 401},
}

heartbeat = {"messageType": "H", "response": {"message": "Success", "code": 200}}

subscription = {
    "messageType": "I",
    "response": {"message": "Success", "code": 200},
    "data": {"subscriptionId": 1234},
}

trade = {
    "messageType": "A",
    "service": "crypto_data",
    "data": [
        "A",
        "BTC",
        "2022-01-01T00:00:00+00:00",
        "Bitfenix",
        123.4,
        567.8,
    ],
}
