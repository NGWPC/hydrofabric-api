import pytest
from unittest.mock import patch, MagicMock
from rest_framework.test import APIRequestFactory
from django.urls import reverse
from .views  import get_modules, modules, moduleMetaData, get_model_parameters_total_count, get_initial_parameters

# Create a factory for making API requests
factory = APIRequestFactory()

@pytest.mark.django_db
@patch('yourapp.views.DatabaseManager')
def test_get_modules_success(mock_db_manager):
    # Arrange
    mock_cursor = MagicMock()
    mock_db_manager.return_value.selectAllModules.return_value = [
        (1, 'Module A'),
        (2, 'Module B')
    ]

    # Act
    request = factory.get(reverse('get_modules'))
    response = get_modules(request)

    # Assert
    assert response.status_code == 200
    assert len(response.data) == 2
    assert response.data[0]['model_id'] == 1
    assert response.data[0]['name'] == 'Module A'


@pytest.mark.django_db
@patch('yourapp.views.DatabaseManager')
def test_get_modules_no_data(mock_db_manager):
    # Arrange
    mock_cursor = MagicMock()
    mock_db_manager.return_value.selectAllModules.return_value = []

    # Act
    request = factory.get(reverse('get_modules'))
    response = get_modules(request)

    # Assert
    assert response.status_code == 404
    assert 'error' in response.data


@pytest.mark.django_db
@patch('yourapp.views.DatabaseManager')
def test_modules_success(mock_db_manager):
    # Arrange
    mock_cursor = MagicMock()
    mock_db_manager.return_value.selectAllModulesDetail.return_value = (
        ['description', 'groups', 'name', 'version_url', 'commit_hash', 'version_number'],
        [
            ("Description A", "Group A", "Module A", "url_A", "hash_A", "1.0"),
            ("Description B", "Group B", "Module B", "url_B", "hash_B", "2.0")
        ]
    )

    # Act
    request = factory.get(reverse('modules'))
    response = modules(request)

    # Assert
    assert response.status_code == 200
    assert len(response.data['modules']) == 2
    assert response.data['modules'][0]['description'] == "Description A"


@pytest.mark.django_db
@patch('yourapp.views.moduleCalibrateData')
@patch('yourapp.views.moduleOutVariablesData')
def test_moduleMetaData_success(mock_out_vars, mock_calibrate_data):
    # Arrange
    mock_calibrate_data.return_value = {
        "calibrate_parameters": [
            {
                "name": "param_A",
                "initial_value": None,
                "description": "Description A",
                "min": 0,
                "max": 10,
                "data_type": "int",
                "units": "m/s"
            }
        ]
    }
    mock_out_vars.return_value = {
        "module_output_variables": [
            {
                "name": "output_var_A",
                "description": "Output Var A"
            }
        ]
    }

    # Act
    request = factory.get(reverse('moduleMetaData', args=['CFE-X']))
    response = moduleMetaData(request, 'CFE-X')

    # Assert
    assert response.status_code == 200
    assert response.data[0]['module_name'] == 'CFE-X'
    assert len(response.data[0]['calibrate_parameters']) == 1
    assert len(response.data[0]['module_output_variables']) == 1


@pytest.mark.django_db
@patch('yourapp.views.DatabaseManager')
def test_get_model_parameters_total_count_success(mock_db_manager):
    # Arrange
    mock_cursor = MagicMock()
    mock_db_manager.return_value.getModelParametersTotalCount.return_value = 42

    # Act
    request = factory.get(reverse('get_model_parameters_total_count', args=['CFE-X']))
    response = get_model_parameters_total_count(request, 'CFE-X')

    # Assert
    assert response.status_code == 200
    assert response.data['total_count'] == 42


@pytest.mark.django_db
@patch('yourapp.views.DatabaseManager')
def test_get_initial_parameters_success(mock_db_manager):
    # Arrange
    mock_cursor = MagicMock()
    mock_db_manager.return_value.selectInitialParameters.return_value = (
        ['name', 'units', 'limits', 'role'],
        [
            ("param_1", "m/s", "0-10", "input"),
            ("param_2", "kg/m3", "0-100", "input")
        ]
    )

    # Act
    request = factory.get(reverse('get_initial_parameters', args=['CFE-X']))
    response = get_initial_parameters(request, 'CFE-X')

    # Assert
    assert response.status_code == 200
    assert len(response.data) == 2
    assert response.data[0]['name'] == "param_1"

