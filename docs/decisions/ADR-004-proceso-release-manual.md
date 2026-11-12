# ADR-004 — Proceso de release manual (sin automatización)

## Estado

Aceptado

## Contexto

El Issue #28 planteaba automatizar la publicación de releases con un workflow
(`.github/workflows/release.yml`) que generara el tag y el release de GitHub
automáticamente al hacer push de un tag o al mergear a `main`.

## Decisión

El profesor confirmó que para este proyecto es suficiente con un tag manual
(etiqueta de Git) en cada cierre de semana, sin necesidad de automatizar el
proceso con un workflow de CI/CD.

En consecuencia, se mantiene el proceso manual ya usado en los releases
`v0.1.0`, `v0.2.0-streaming` y `v0.3.0-orchestration`:

1. Verificar que `main` tiene CI verde y todos los PRs de la semana mergeados.
2. Crear el release desde la interfaz de GitHub (`Releases > Draft a new release`),
   generando el tag correspondiente (ej. `v0.3.0-orchestration`) apuntando a `main`.
3. Documentar en la descripción del release qué se entregó, qué PRs se incluyeron
   y dónde está la evidencia.

## Alternativas descartadas

- **Workflow `release.yml` automatizado** (por ejemplo, disparado por
  `on: push: tags: ['v*']` que genere el release con `actions/create-release` o
  similar): se descarta porque añade complejidad de mantenimiento
  (configuración de permisos, plantillas de notas de release) sin un beneficio
  claro para un equipo de 3 personas con pocos releases por semana, y porque el
  profesor indicó explícitamente que no es un requisito del curso.

## Consecuencias

- Los releases se seguirán creando manualmente desde GitHub, con la plantilla
  de descripción ya usada en los releases anteriores.
- Si el equipo decide automatizarlo en el futuro (por ejemplo, para el release
  final `v1.0.0` de la Semana 4), este ADR puede revisarse y actualizarse.
