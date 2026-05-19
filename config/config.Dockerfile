# The config image contains instructions for Antithesis for how to set up your
# system. At minimum it consists of a `docker-compose.yml`. Additional files
# used dufing setup may be added later.

# Start from an empty image:
FROM scratch

# Copy docker-compose.yml, environment file, and the required config files:
COPY ./docker-compose.yaml docker-compose.yaml