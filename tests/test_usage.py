from molq import submit


class TestRegisterCluster:
    def test_register(self):
        # Clear any existing clusters from previous tests
        initial_count = submit.get_n_clusters()

        # register cluster
        @submit("cluster_alpha", "slurm")
        def foo(a: int, b: int):
            job_id = yield dict()
            return job_id

        assert submit.get_n_clusters() == initial_count + 1

        # reuse without config
        @submit("cluster_alpha")
        def bar(a: int, b: int):
            job_id = yield dict()

        # Should still be the same count since we're reusing the same cluster
        assert submit.get_n_clusters() == initial_count + 1

    def test_get_cluster_external(self):
        cluster_alpha = submit.get_cluster("cluster_alpha")
        assert cluster_alpha is submit.CLUSTERS["cluster_alpha"]
