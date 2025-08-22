import unittest
from unittest.mock import MagicMock, patch, call, ANY

from snakemake_executor_plugin_googlebatch.executor import GoogleBatchExecutor
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from google.cloud import batch_v1
from google.api_core.exceptions import DeadlineExceeded, ResourceExhausted


class TestGoogleBatchExecutor(unittest.TestCase):
    def setUp(self):
        self.workflow = MagicMock()
        self.workflow.persistence.path = "/path/to/workflow"
        self.workflow.main_snakefile = "/path/to/workflow/Snakefile"
        self.workflow.remote_execution_settings.container_image = "default/container"
        self.workflow.remote_execution_settings.preemptible_rules.is_preemptible.return_value = False
        self.workflow.remote_execution_settings.preemptible_retries = 3
        self.workflow.spawned_job_args_factory.general_args.return_value = "--cores 1"
        self.workflow.spawned_job_args_factory.precommand.return_value = "export FOO=bar"
        self.workflow.spawned_job_args_factory.envvars.return_value = {"SNAKEMAKE_TEST": "1"}
        self.workflow.group_settings.local_groupid = "local_group"
        self.workflow.executor_settings = MagicMock()

        self.job = MagicMock()
        self.job.name = "test_job"
        self.job.rule.name = "test_rule"
        self.job.resources = {}
        self.job.is_group.return_value = False
        self.job.logfile_suggestion.return_value = ".snakemake/googlebatch_logs/test_job.log"

        self.executor_settings = MagicMock()
        self.executor_settings.project = "test-project"
        self.executor_settings.region = "us-central1"
        self.executor_settings.machine_type = "e2-standard-2"
        self.executor_settings.image_family = "hpc-centos-7"
        self.executor_settings.image_project = "cloud-hpc-image-public"
        self.executor_settings.cpu_milli = 1000
        self.executor_settings.memory = 1024
        self.executor_settings.labels = ""
        self.executor_settings.bucket = None
        self.executor_settings.mount_path = "/mnt/gcs"
        self.executor_settings.retry_count = 0
        self.executor_settings.max_run_duration = "3600s"
        self.executor_settings.work_tasks_per_node = 1
        self.executor_settings.work_tasks = 1
        self.executor_settings.network = None
        self.executor_settings.subnetwork = None
        self.executor_settings.service_account = None
        self.executor_settings.boot_disk_image = None
        self.executor_settings.boot_disk_gb = None
        self.executor_settings.boot_disk_type = None
        self.executor_settings.snippets = None
        self.executor_settings.entrypoint = None
        self.executor_settings.split_commands = False
        self.executor_settings.docker_username = None
        self.executor_settings.docker_password = None

        self.workflow.executor_settings = self.executor_settings

        with patch("snakemake_executor_plugin_googlebatch.executor.batch_v1.BatchServiceClient") as self.mock_batch_client, \
             patch("os.path.realpath", return_value="/path/to/workflow"), \
             patch("os.path.dirname", return_value="/path/to"):
            self.executor = GoogleBatchExecutor(
                workflow=self.workflow,
                logger=MagicMock(),
                executor_settings=self.executor_settings,
            )
            self.executor.batch = self.mock_batch_client.return_value
            self.executor.report_job_submission = MagicMock()
            self.executor.report_job_success = MagicMock()
            self.executor.report_job_error = MagicMock()

    def test_post_init_success(self):
        self.mock_batch_client.assert_called_once()
        self.assertIsNotNone(self.executor.batch)

    def test_post_init_failure(self):
        with patch("snakemake_executor_plugin_googlebatch.executor.batch_v1.BatchServiceClient", side_effect=Exception("Connection failed")):
            with self.assertRaises(WorkflowError):
                GoogleBatchExecutor(
                    workflow=self.workflow,
                    logger=MagicMock(),
                    executor_settings=self.executor_settings,
                )

    def test_get_param(self):
        self.assertEqual(self.executor.get_param(self.job, "project"), "test-project")
        self.executor_settings.project = "cli-project"
        self.assertEqual(self.executor.get_param(self.job, "project"), "cli-project")
        self.job.resources["googlebatch_project"] = "job-project"
        self.assertEqual(self.executor.get_param(self.job, "project"), "job-project")

    @patch("snakemake_executor_plugin_googlebatch.utils.read_file", return_value="snakefile content")
    @patch("os.path.exists", return_value=True)
    @patch("os.makedirs")
    def test_run_job_simple(self, mock_makedirs, mock_exists, mock_read_file):
        self.executor.batch.create_job.return_value = MagicMock(name="created_job", uid="job-uid")
        self.executor.run_job(self.job)

        mock_makedirs.assert_called_once()
        self.executor.batch.create_job.assert_called_once()
        create_request = self.executor.batch.create_job.call_args.args[0]

        self.assertEqual(create_request.parent, "projects/test-project/locations/us-central1")
        self.assertTrue(create_request.job_id.startswith("test-job-"))
        
        batchjob = create_request.job
        self.assertEqual(batchjob.labels, {"snakemake-job": "test-job"})
        self.assertEqual(len(batchjob.task_groups), 1)
        
        task_spec = batchjob.task_groups[0].task_spec
        self.assertEqual(task_spec.max_retry_count, 0)
        self.assertEqual(task_spec.max_run_duration, "3600s")
        self.assertEqual(task_spec.compute_resource.cpu_milli, 1000)
        self.assertEqual(task_spec.compute_resource.memory_mib, 1024)
        
        # setup, snakefile_step, barrier, runnable
        self.assertEqual(len(task_spec.runnables), 4)
        
        runnable = task_spec.runnables[3]
        self.assertTrue("snakemake" in runnable.script.text)
        self.assertEqual(runnable.environment.variables, {"SNAKEMAKE_TEST": "1"})

        self.executor.report_job_submission.assert_called_once()
        submitted_job_info = self.executor.report_job_submission.call_args[0][0]
        self.assertEqual(submitted_job_info.external_jobid, "created_job")

    @patch("snakemake_executor_plugin_googlebatch.utils.read_file", return_value="snakefile content")
    @patch("os.path.exists", return_value=True)
    @patch("os.makedirs")
    def test_run_job_container(self, mock_makedirs, mock_exists, mock_read_file):
        self.job.resources["googlebatch_image_family"] = "batch-cos-stable"
        self.executor.batch.create_job.return_value = MagicMock(name="created_job_container", uid="job-uid-cont")
        self.executor.run_job(self.job)

        self.executor.batch.create_job.assert_called_once()
        create_request = self.executor.batch.create_job.call_args.args[0]
        runnable = create_request.job.task_groups[0].task_spec.runnables[3]
        
        self.assertTrue(runnable.HasField("container"))
        self.assertEqual(runnable.container.image_uri, "default/container")
        self.assertEqual(runnable.container.entrypoint, "/bin/bash")
        self.assertIn("/tmp/workdir/Snakefile", runnable.container.commands[0])

    @patch("snakemake_executor_plugin_googlebatch.utils.read_file", return_value="snakefile content")
    @patch("os.path.exists", return_value=True)
    @patch("os.makedirs")
    def test_run_job_preemptible(self, mock_makedirs, mock_exists, mock_read_file):
        self.workflow.remote_execution_settings.preemptible_rules.is_preemptible.return_value = True
        self.executor.batch.create_job.return_value = MagicMock(name="created_job_preempt", uid="job-uid-preempt")
        self.executor.run_job(self.job)

        self.executor.batch.create_job.assert_called_once()
        create_request = self.executor.batch.create_job.call_args.args[0]
        
        policy = create_request.job.allocation_policy
        self.assertEqual(policy.instances[0].policy.provisioning_model, 3) # PREEMPTIBLE
        
        task_spec = create_request.job.task_groups[0].task_spec
        self.assertEqual(task_spec.max_retry_count, 3) # from preemptible_retries

    @patch("snakemake_executor_plugin_googlebatch.utils.read_file", return_value="snakefile content")
    @patch("os.path.exists", return_value=True)
    @patch("os.makedirs")
    def test_run_job_with_storage(self, mock_makedirs, mock_exists, mock_read_file):
        self.executor_settings.bucket = "my-bucket"
        self.executor.batch.create_job.return_value = MagicMock(name="created_job_storage", uid="job-uid-storage")
        self.executor.run_job(self.job)

        self.executor.batch.create_job.assert_called_once()
        create_request = self.executor.batch.create_job.call_args.args[0]
        
        task_spec = create_request.job.task_groups[0].task_spec
        self.assertEqual(len(task_spec.volumes), 1)
        volume = task_spec.volumes[0]
        self.assertEqual(volume.gcs.remote_path, "my-bucket")
        self.assertEqual(volume.mount_path, "/mnt/gcs")

    @patch("snakemake_executor_plugin_googlebatch.utils.read_file", return_value="snakefile content")
    @patch("os.path.exists", return_value=True)
    @patch("os.makedirs")
    def test_run_job_with_gpu(self, mock_makedirs, mock_exists, mock_read_file):
        self.job.resources["nvidia_gpu"] = 2
        self.executor.batch.create_job.return_value = MagicMock(name="created_job_gpu", uid="job-uid-gpu")
        self.executor.run_job(self.job)

        self.executor.batch.create_job.assert_called_once()
        create_request = self.executor.batch.create_job.call_args.args[0]
        
        policy = create_request.job.allocation_policy
        instance_policy = policy.instances[0].policy
        self.assertTrue(policy.instances[0].install_gpu_drivers)
        self.assertEqual(len(instance_policy.accelerators), 1)
        accelerator = instance_policy.accelerators[0]
        self.assertEqual(accelerator.type_, "nvidia-tesla-t4")
        self.assertEqual(accelerator.count, 2)

    async def test_check_active_jobs_succeeded(self):
        job_info = SubmittedJobInfo(
            job=self.job,
            external_jobid="job1",
            aux={"logfile": "log1", "last_seen": None, "batch_job": MagicMock()}
        )
        
        mock_response = MagicMock()
        mock_response.status.state = batch_v1.JobStatus.State.SUCCEEDED
        mock_response.status.status_events = []
        self.executor.batch.get_job.return_value = mock_response
        
        active_jobs = [job_info]
        remaining_jobs = [j async for j in self.executor.check_active_jobs(active_jobs)]
        
        self.executor.batch.get_job.assert_called_once_with(request=batch_v1.GetJobRequest(name="job1"))
        self.assertEqual(len(remaining_jobs), 0)
        self.executor.report_job_success.assert_called_once_with(job_info)

    async def test_check_active_jobs_failed(self):
        job_info = SubmittedJobInfo(
            job=self.job,
            external_jobid="job2",
            aux={"logfile": "log2", "last_seen": None, "batch_job": MagicMock()}
        )
        
        mock_response = MagicMock()
        mock_response.status.state = batch_v1.JobStatus.State.FAILED
        mock_response.status.status_events = []
        self.executor.batch.get_job.return_value = mock_response
        
        active_jobs = [job_info]
        remaining_jobs = [j async for j in self.executor.check_active_jobs(active_jobs)]
        
        self.executor.batch.get_job.assert_called_once_with(request=batch_v1.GetJobRequest(name="job2"))
        self.assertEqual(len(remaining_jobs), 0) # Job is finished, not remaining
        self.executor.report_job_error.assert_called_once()

    async def test_check_active_jobs_running(self):
        job_info = SubmittedJobInfo(
            job=self.job,
            external_jobid="job3",
            aux={"logfile": "log3", "last_seen": None, "batch_job": MagicMock()}
        )
        
        mock_response = MagicMock()
        mock_response.status.state = batch_v1.JobStatus.State.RUNNING
        mock_response.status.status_events = []
        self.executor.batch.get_job.return_value = mock_response
        
        active_jobs = [job_info]
        remaining_jobs = [j async for j in self.executor.check_active_jobs(active_jobs)]
        
        self.executor.batch.get_job.assert_called_once_with(request=batch_v1.GetJobRequest(name="job3"))
        self.assertEqual(len(remaining_jobs), 1)
        self.assertEqual(remaining_jobs[0], job_info)
        self.executor.report_job_success.assert_not_called()
        self.executor.report_job_error.assert_not_called()

    def test_cancel_jobs(self):
        job1 = SubmittedJobInfo(job=self.job, external_jobid="job1")
        job2 = SubmittedJobInfo(job=self.job, external_jobid="job2")
        active_jobs = [job1, job2]
        
        mock_operation = MagicMock()
        mock_operation.result.return_value = "done"
        self.executor.batch.delete_job.return_value = mock_operation
        
        self.executor.cancel_jobs(active_jobs)
        
        self.executor.batch.delete_job.assert_has_calls([
            call(request=batch_v1.DeleteJobRequest(name="job1", reason=ANY)),
            call(request=batch_v1.DeleteJobRequest(name="job2", reason=ANY)),
        ])

    @patch("snakemake_executor_plugin_googlebatch.executor.logging.Client")
    def test_save_finished_job_logs(self, mock_logging_client):
        job_info = SubmittedJobInfo(
            job=self.job,
            external_jobid="job1",
            aux={"logfile": "log.txt", "batch_job": MagicMock(uid="job-uid")}
        )
        
        mock_logger = MagicMock()
        mock_logging_client.return_value.logger.return_value = mock_logger
        
        with patch("builtins.open", unittest.mock.mock_open()) as mock_file:
            self.executor.save_finished_job_logs(job_info)
            mock_file.assert_called_once_with("log.txt", "w", encoding="utf-8")
            mock_logger.list_entries.assert_called_once()

    @patch("snakemake_executor_plugin_googlebatch.executor.logging.Client")
    @patch("time.sleep")
    def test_save_finished_job_logs_resource_exhausted(self, mock_sleep, mock_logging_client):
        job_info = SubmittedJobInfo(
            job=self.job,
            external_jobid="job1",
            aux={"logfile": "log.txt", "batch_job": MagicMock(uid="job-uid")}
        )
        
        mock_logger = MagicMock()
        mock_logger.list_entries.side_effect = [ResourceExhausted("too many requests"), []]
        mock_logging_client.return_value.logger.return_value = mock_logger
        
        with patch("builtins.open", unittest.mock.mock_open()) as mock_file:
            self.executor.save_finished_job_logs(job_info)
            self.assertEqual(mock_logger.list_entries.call_count, 2)
            mock_sleep.assert_called_once_with(60)

if __name__ == '__main__':
    unittest.main()
