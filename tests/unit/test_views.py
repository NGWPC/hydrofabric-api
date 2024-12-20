def test_version_endpoint(client):
    # Test with trailing slash
    response = client.get('/version/')
    assert response.status_code == 200

    # Check the conftest version value
    response_data = response.json()
    assert response_data['version'] == '1.0.0'

    # Test redirect behavior
    redirect_response = client.get('/version', follow=False)
    assert redirect_response.status_code == 301
    assert redirect_response['Location'] == '/version/'

    # Test following the redirect
    followed_response = client.get('/version', follow=True)
    assert followed_response.status_code == 200
