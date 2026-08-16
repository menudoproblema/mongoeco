# ADR-011 - Provenance runtime fuera del documento BSON

## Contexto

Search produce scores, highlights y aliases virtuales que deben sobrevivir a
parte de un pipeline sin convertirse por accidente en datos persistidos. Un
sidecar insertado en el documento y los subtipos de valores permiten cubrir
casos simples, pero acoplan provenance a nombres, tipos Python y operaciones de
`deepcopy`. Ese modelo no define correctamente fan-out, joins, facets ni
writeback recursivo.

## Decision

El runtime de aggregation usa un `RuntimeDocumentState` interno que mantiene
separados el documento BSON ordinario, la metadata runtime y los campos
virtuales con su politica de materializacion. Ningun valor BSON se etiqueta
mediante subtipos ni se inserta un namespace privado en el documento.

Los stages declaran una transformacion de provenance:

- los stages de seleccion, orden y ventana preservan el estado;
- las referencias explicitas a metadata producen datos ordinarios;
- projection y transformaciones conservan solo campos virtuales incluidos de
  forma implicita y materializan expresiones explicitas;
- unwind divide y rebasa provenance;
- group materializa sus acumuladores y no propaga metadata implicita;
- facet, lookup y union mantienen scopes independientes;
- salida publica materializa aliases virtuales en una copia;
- persistencia descarta toda metadata no materializada.

Un campo real siempre tiene prioridad sobre un alias virtual. Los snapshots,
batches y copias toman ownership defensivo del estado completo.

## Consecuencias

- `RuntimeVirtualList` y los sidecars BSON dejan de ser la representacion
  canonica.
- Paths y expresiones reciben un estado de evaluacion explicito cuando existe.
- Los stages estructurales necesitan pruebas de provenance, no solo de valor.
- Engines externos siguen intercambiando documentos ordinarios y metadata
  tipada; nunca necesitan conocer namespaces internos.

## Alternativas descartadas

- Crear un wrapper por cada tipo BSON.
- Rastrear objetos por identidad global o weak references.
- Inferir persistencia por el nombre del campo de destino.
- Limpiar sidecars recursivamente despues de ejecutar un pipeline que ya ha
  perdido la provenance original.
