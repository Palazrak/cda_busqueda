# CI/CD Readiness

This repository starts with CI only. Deployment to AWS ECS/ECR is intentionally
deferred until AWS credentials, ECR repositories, ECS services, and secret
management are available.

## GitHub Actions

The CI workflow runs on pull requests to `main` and pushes to `main`.

It checks:

- Python unit tests with `python -m unittest discover -s tests`
- Python syntax compilation across `scripts`, `scheduler`, `backend`, and `tests`
- Frontend install, lint, typecheck, and production build
- Docker Compose config validity
- Docker builds for `scheduler`, `backend`, and `frontend`
- Compose smoke test for `postgres`, `postgrest`, `backend`, and `frontend`

The scheduler is not started in CI. It runs live scrapers against third-party
sites, which makes CI slow, flaky, and potentially noisy for those sources.
Scheduler behavior is covered through unit tests and Docker import/build checks.

## Local CI-Equivalent Commands

Run these before opening a pull request:

```bash
python -m unittest discover -s tests
python -m compileall -q scripts scheduler backend tests
docker compose config --quiet
docker compose build scheduler backend frontend
```

Frontend checks can be run locally when Node.js/npm are available:

```bash
cd frontend
npm ci
npm run lint
npx tsc --noEmit
npm run build
```

## AWS Credentials

CI does not require AWS credentials. Scraper S3 uploads stay disabled by default
with `S3_UPLOAD_ENABLED=false`. Advanced backend face-search returns a controlled
503 response until Rekognition credentials are configured.

When AWS is ready, add a separate CD workflow that authenticates to AWS, builds
images, pushes them to ECR, and deploys the existing container services to ECS.

## Database Schema

`init.sql` remains the source for fresh local and CI databases. Before production
data must be preserved, add versioned SQL migrations and make deployment run
those migrations before new application containers are promoted.
