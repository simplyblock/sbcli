"""`ObjectStore` / `S3Bucket` coverage: everything dev's own suite leaves open.

Dev's side is strong where it is cheap to be: 38 unit scenarios, 18 of 20
integration scenarios against ``envtest``, and four live E2E specs covering S3
protocol conformance (MinIO ``mint``), a seeded model-checker, and ``warp``
load. None of it is rebuilt here.

What none of it has is a cluster that breaks. Every spec in
``csi-driver/e2e/s3.go`` runs against a healthy cluster -- dev's own two test
plans say so, with every E2E row marked ``—`` and three gaps carrying no owner
at all. On top of that, ``envtest`` has no kubelet and no garbage collector, so
a handful of scenarios dev *wrote* are still unproven: an ``envFrom`` Secret
that does not exist degrades only when a real kubelet reports it, and owned
objects are only actually collected on a real API server.

Those two categories are this package. The classes end in ``K8s`` because the
platform filter in ``e2e.py`` keys on that suffix: the CRDs exist only on the
k8s path, so a Docker run reports them skipped instead of failing obscurely.

Every case drives the CRDs through ``kubectl``, never ``sbctl``. On the k8s
path, ``sbctl`` skips the operator and the CSI driver, which are the two
components under test here.
"""
