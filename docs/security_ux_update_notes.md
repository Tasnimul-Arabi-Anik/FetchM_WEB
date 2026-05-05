# FetchM Web Security and UX Update Notes

Date: 2026-05-05

## Scope

This update is intentionally small and reversible. It addresses sign-in/register clarity, basic accessibility, password policy visibility, session lifetime, and admin-facing security posture without changing the job, metadata, or standardization pipelines.

## Changes

- Login now includes a short FetchM Web purpose panel explaining job submission, logs, downloads, and why account separation is required.
- Login/register/reset forms now use explicit input IDs, label bindings, and helper text for screen readers.
- New passwords must be at least 10 characters and include at least one letter and one number.
- Login sessions are permanent Flask sessions with a configurable lifetime through `FETCHM_WEBAPP_SESSION_HOURS` (default: 12 hours).
- Session cookie posture is surfaced in the admin overview: `HttpOnly`, `Secure`, `SameSite`, session lifetime, password policy, password reset configuration, and admin model.
- Admin user review explicitly avoids exposing passwords or password hashes and correctly labels the current logged-in user.

## Configuration

- `FETCHM_WEBAPP_SECURE_COOKIE=1` enables secure session cookies for HTTPS deployments.
- `FETCHM_WEBAPP_SESSION_HOURS` controls server-side session lifetime in hours.
- Existing SMTP settings still control password reset availability.

## Rollback

After this update is committed, the safe rollback path is:

```bash
git revert <commit-sha>
```

This rollback should restore the previous login/register/admin behavior without touching metadata standardization outputs or generated data.

## Deferred Items

The following are useful but intentionally not included in this slice because they require broader product/security decisions:

- Multi-factor authentication.
- Social login through Google or GitHub.
- Role-based admin permissions beyond the current single admin role.
- Admin user deactivation/password-reset workflows.
- Rate limits, quotas, and API token management.
- Full audit logging of user logins, downloads, and admin actions.
