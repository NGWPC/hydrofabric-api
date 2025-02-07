import sys
from pathlib import Path
import django
from django.conf import settings

# Get the root directory of your project
root_dir = Path(__file__).parent.parent

# Add the root directory and the django apps directory to Python path
sys.path.append(str(root_dir))
sys.path.append(str(root_dir / 'djangoApps'))


def pytest_configure():
    settings.configure(
        S3_BUCKET='test-bucket',
        VERSION='1.0.0',
        DEBUG=True,
        DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': ':memory:'
            }
        },
        INSTALLED_APPS=[
            'django.contrib.contenttypes',
            'django.contrib.auth',
            'django.contrib.sessions',
            'django.contrib.messages',
            'rest_framework',
            'djangoApps.init_param_app',
        ],
        SECRET_KEY='test-key-not-for-production',
        MIDDLEWARE=[
            'django.middleware.security.SecurityMiddleware',
            'django.contrib.sessions.middleware.SessionMiddleware',
            'django.middleware.common.CommonMiddleware',
            'django.middleware.csrf.CsrfViewMiddleware',
            'django.contrib.auth.middleware.AuthenticationMiddleware',
            'django.contrib.messages.middleware.MessageMiddleware',
        ],
        ROOT_URLCONF='djangoApps.init_param_app.urls',
        TEMPLATES=[
            {
                'BACKEND': 'django.template.backends.django.DjangoTemplates',
                'DIRS': [],
                'APP_DIRS': True,
                'OPTIONS': {
                    'context_processors': [
                        'django.template.context_processors.debug',
                        'django.template.context_processors.request',
                        'django.contrib.auth.context_processors.auth',
                        'django.contrib.messages.context_processors.messages',
                    ],
                },
            },
        ],
        USE_TZ=True,
        TIME_ZONE='UTC',
        LANGUAGE_CODE='en-us',
    )
    django.setup()
