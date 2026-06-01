## Description
Provide a clear description of the changes.

---

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Refactor
- [ ] Documentation
- [ ] Test
- [ ] Breaking change
- [ ] New migration

---

## Testing
- [ ] Unit tests added/updated (`make test-unit`)
- [ ] Integration tests added/updated (`make test-int`)
- [ ] All tests passing with race detector (`make race`)
- [ ] Manual testing performed (if required)

---

## Checklist

### Code Quality
- [ ] Code is properly formatted (`gofmt`)
- [ ] Linting passed (`make lint`)
- [ ] Vet passed (`make vet`)
- [ ] No debug logs / commented-out code
- [ ] Exported symbols have godoc comments

### Database / Migrations
- [ ] New migrations have matching `.up.sql` and `.down.sql`
- [ ] Down migration correctly reverses the up migration
- [ ] RLS policies tested with `FORCE ROW LEVEL SECURITY` where applicable

### Security
- [ ] No secrets or DSNs hardcoded
- [ ] GUC values are not logged (no credential leakage in slow-query output)
- [ ] New config fields documented in README env-vars table

### Documentation
- [ ] README updated (if public API or env vars changed)
- [ ] `CHANGELOG.md` `[Unreleased]` section updated
- [ ] `VERSIONING.md` impact assessed if `pkg/` changed

---

## Related Issue
Closes #<issue-id>

---

## Deployment Notes
Mention anything important for consumers upgrading (migration steps, config changes, etc.).
