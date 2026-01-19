from typing import List, Union
from kubernetes import client, config, watch
from kubernetes.client.models import V1ContainerState, V1Container, V1ContainerStatus
from kubernetes.client.rest import ApiException
from kubernetes.config.config_exception import ConfigException
from calrissian.executor import IncompleteStatusException
from calrissian.retry import retry_exponential_if_exception_type
import threading
import logging
import os
from urllib3.exceptions import HTTPError
from datetime import datetime, timezone

log = logging.getLogger('calrissian.k8s')

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# When running inside a pod, kubernetes puts the namespace in a text file at this location
K8S_NAMESPACE_FILE = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

# Environment variable that will receive the name of this pod so we can lookup volume details
POD_NAME_ENV_VARIABLE = 'CALRISSIAN_POD_NAME'

# Namespace to use if not running in cluster
K8S_FALLBACK_NAMESPACE = 'default'

# Default max number of none status states for pod to start initiliaze
MAX_STATUS_NONE_COUNTS = 6

WATCH_TIMEOUT_S = 30  # short, bounded HTTP watch

def read_file(path):
    with open(path) as f:
        return f.read()


def load_config_get_namespace():
    try:
        config.load_incluster_config() # raises if not in cluster
        namespace = read_file(K8S_NAMESPACE_FILE)
    except ConfigException:
        config.load_kube_config()
        namespace = K8S_FALLBACK_NAMESPACE
    return namespace


class CalrissianJobException(Exception):
    pass


class CompletionResult(object):
    """
    Simple structure to hold information about pod execution duration and resources.
    The CPU and memory values should be in kubernetes units (strings).
    """

    def __init__(self, exit_code, cpus, memory, start_time, finish_time, tool_log, error_msg=None):
        self.exit_code = exit_code
        self.cpus = cpus
        self.memory = memory
        self.start_time = start_time
        self.finish_time = finish_time
        self.tool_log = tool_log
        self.error_msg = error_msg

SIDECAR_PREFIXES = ('vault-agent',)

class KubernetesClient(object):
    """
    Instances of this class are created by a `calrissian.job.CalrissianCommandLineJob`,
    which are often running in background threads (spawned by `calrissian.executor.MultithreadedJobExecutor`

    This class uses a PodMonitor to keep track of the pods it submits in a single, shared list.
    KubernetesClient is responsible for telling PodMonitor after it has submitted a pod and when it knows that pod
    is terminated. Using PodMonitor as a context manager (with PodMonitor() as p) acquires a lock for thread safety.

    """
    def __init__(self):
        self.pod = None
        # load_config must happen before instantiating client
        self.completion_result = None
        self.namespace = load_config_get_namespace()
        self.core_api_instance = client.CoreV1Api()
        self.tool_log = []
        self.counters = {"Pending": 0,
                         "InitContainer": 0,
                         "DockerImageError": 0}

    @retry_exponential_if_exception_type((ApiException, HTTPError,), log)
    def submit_pod(self, pod_body):
        with PodMonitor() as monitor:
            pod = self.core_api_instance.create_namespaced_pod(self.namespace, pod_body)
            log.info('Created k8s pod name {} with id {}'.format(pod.metadata.name, pod.metadata.uid))
            monitor.add(pod)
            self._set_pod(pod)

    def should_delete_pod(self):
        """
        Decide whether or not to delete a pod. Defaults to True if unset.
        Checks the CALRISSIAN_DELETE_PODS environment variable
        :return:
        """
        delete_pods = os.getenv('CALRISSIAN_DELETE_PODS', '')
        if str.lower(delete_pods) in ['false', 'no', '0']:
            return False
        else:
            return True

    @retry_exponential_if_exception_type((ApiException, HTTPError,), log)
    def delete_pod_name(self, pod_name):
        try:
            self.core_api_instance.delete_namespaced_pod(pod_name, self.namespace)
        except ApiException as e:
            if e.status == 404:
                # pod was not found - already deleted, so do not retry
                pass
            else:
                # Re-raise
                raise

    def _handle_completion(self, state: V1ContainerState, container: V1Container):
        """
        Sets self.completion_result to an object containing exit_code, resources, and timingused
        :param state: V1ContainerState
        :param container: V1Container
        :return: None
        """
        
        exit_code = state.terminated.exit_code
        # We extract resource requests here since requests are used for scheduling. Limits are
        # not used for scheduling and not specified in our submitted pods
        cpus, memory = self._extract_cpu_memory_requests(container)
        start_time, finish_time = self._extract_start_finish_times(state)

        self.completion_result = CompletionResult(
            exit_code,
            cpus,
            memory,
            start_time,
            finish_time, 
            self.tool_log,
        )
        log.info('handling completion with {}'.format(exit_code))

    @staticmethod
    def format_log_entry(pod_name, log_entry):
        return {"timestamp": f"{datetime.utcnow().isoformat()}Z", "pod": pod_name, "entry": log_entry}

    @retry_exponential_if_exception_type((ApiException, HTTPError,), log)
    def follow_logs(self, container_name: str = None):
        pod_name = self.pod.metadata.name
        log.info('[{}] follow_logs start (container={})'.format(pod_name, container_name or '<default>'))
        stream = self.core_api_instance.read_namespaced_pod_log(
            self.pod.metadata.name,
            self.namespace,
            container=container_name,
            follow=True,
            _preload_content=False
        ).stream()

        for line in stream:
            # .stream() is only available if _preload_content=False
            # .stream() returns a generator, each iteration yields bytes.
            # kubernetes-client decodes them as utf-8 when _preload_content is True
            # https://github.com/kubernetes-client/python/blob/fcda6fe96beb21cd05522c17f7f08c5a7c0e3dc3/kubernetes/client/rest.py#L215-L216
            # So we do the same here
            line = line.decode('utf-8', errors="ignore").rstrip()
            log.debug('[{}] {}'.format(pod_name, line))
            self.tool_log.append(self.format_log_entry(pod_name, line))
        log.info('[{}] follow_logs end'.format(pod_name))


    @staticmethod
    def _pick_main_container_name(pod) -> str:
        """Choose the workload container.
        Priority: env override -> first non-sidecar -> first in spec."""
        #log.info('_pick_main_container_name list: {} '.format(str(pod.spec.containers)))

        for c in (pod.spec.containers or []):
            if not any(c.name.startswith(pfx) for pfx in SIDECAR_PREFIXES):
                return c.name
        # fallback
        return pod.spec.containers[0].name

    @staticmethod
    def _get_container_status_by_name(pod, name) -> V1ContainerStatus:
        for s in (pod.status.container_statuses or []):
            if s.name == name:
                return s
        return None

    @staticmethod
    def _get_container_spec_by_name(pod, name) -> V1Container:
        for c in (pod.spec.containers or []):
            if c.name == name:
                return c
        return None

    @staticmethod
    def _any_init_container_failing(pod) -> str:
        """Return a reason string if an init container is failing."""
        for s in (pod.status.init_container_statuses or []):
            st = s.state
            if st and st.waiting and st.waiting.reason in ('CrashLoopBackOff', 'Error'):
                return f"init container '{s.name}' is {st.waiting.reason}"
            if st and st.terminated and s.restart_count > 0 and (st.terminated.exit_code != 0):
                return f"init container '{s.name}' terminated: {st.terminated.reason or st.terminated.exit_code}"
        return None


    def _make_failure_result(self, pod_name: str, reason: str) -> CompletionResult:
        """
        Build a CompletionResult that the reporter can always parse.
        - exit_code: -1 (our convention for infra/pod-level failure)
        - cpus/memory: valid k8s strings so TimedResourceReport won't explode
        """
        now = datetime.now(timezone.utc)
        zoo_message = {"exit_code": -1, "step": pod_name, "error_msg": reason}
        return CompletionResult(
            exit_code=-1,
            cpus="0m",               # must be a string, not int
            memory="0Mi",            # must be a string, not int
            start_time=now,
            finish_time=now,
            tool_log=self.tool_log,
            error_msg=zoo_message if pod_name else reason,
        )



    @retry_exponential_if_exception_type((ApiException, HTTPError, CalrissianJobException, IncompleteStatusException), log)
    def wait_for_completion(self) -> CompletionResult:
        w = watch.Watch()

        for event in w.stream(
            self.core_api_instance.list_namespaced_pod,
            self.namespace,
            field_selector=self._get_pod_field_selector(),
            timeout_seconds=WATCH_TIMEOUT_S,
        ):
            pod = event["object"]
            pod_name = pod.metadata.name
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            log.info("%s wait_for_completion pods object %s", ts, pod_name)
            log.info("------------------------------------------------------------------------------------------------")
            log.info("Pod status object is= %s", str(pod.status))
            log.info("------------------------------------------------------------------------------------------------")

            # -----------------------------------------------------------
            # 1) Detect pod scheduling / pending issues early
            # -----------------------------------------------------------
            if pod.status and pod.status.phase == "Pending":
                # see if it's explicitly unschedulable
                unschedulable_reason = None
                for cond in pod.status.conditions or []:
                    if cond.type == "PodScheduled" and cond.status == "False":
                        unschedulable_reason = f"{cond.reason}: {cond.message}"

                # generic pending → try a few times
                log.info(
                    "wait_for_completion pod %s is Pending counts: %s max: %s reason: %s",
                    pod_name,
                    str(self.counters["Pending"]),
                    str(MAX_STATUS_NONE_COUNTS),
                    str(unschedulable_reason),
                )

                if unschedulable_reason and  self.counters["Pending"] < MAX_STATUS_NONE_COUNTS:
                    self.counters["Pending"] += 1
                    continue
                elif unschedulable_reason:
                    log.info("%s wait_for_completion pod %s cannot be scheduled reason %s: ", ts, pod_name, unschedulable_reason)
                    self.completion_result = self._make_failure_result(
                        pod_name,
                        reason=f"Pod unschedulable: {unschedulable_reason}",
                    )
                    w.stop()
                    break


            # -----------------------------------------------------------
            # 2) From here on we expect to deal with the main container
            # -----------------------------------------------------------
            main_name = self._pick_main_container_name(pod)
            container_status = self._get_container_status_by_name(pod, main_name)
            log.info(
                "wait_for_completion pod name %s with container name %s with id %s has status %s",
                pod_name,
                main_name,
                pod.metadata.uid,
                container_status,
            )

            # container status can still be None here (pod in transition)
            if container_status is None:
                log.info(
                    "wait_for_completion container_status is None for pod %s – counts: %s max: %s",
                    pod_name,
                    str(self.counters["InitContainer"]),
                    str(MAX_STATUS_NONE_COUNTS),
                )
                init_reason = self._any_init_container_failing(pod)

                if init_reason:
                    log.error("wait_for_completion init_reason error %s: %s", pod_name, init_reason)
                    # best-effort to fetch init logs
                    try:
                        init_names = [s.name for s in (pod.status.init_container_statuses or [])]
                        for iname in init_names:
                            try:
                                ilog = self.core_api_instance.read_namespaced_pod_log(
                                    pod_name,
                                    self.namespace,
                                    container=iname,
                                    previous=True,
                                    tail_lines=200,
                                )
                                for ln in ilog.splitlines():
                                    self.tool_log.append(self.format_log_entry(pod_name, f"[init:{iname}] {ln}"))
                            except Exception:
                                pass
                    finally:
                        self.completion_result = self._make_failure_result(
                            pod_name,
                            reason=f"Init container failed: {init_reason}",
                        )
                        w.stop()
                    break
                elif self.counters["InitContainer"]< MAX_STATUS_NONE_COUNTS:
                    self.counters["InitContainer"] += 1
                    continue
                else:
                    self.completion_result = self._make_failure_result(
                        pod_name,
                        reason="Pod container status stayed None for too long",
                    )
                    w.stop()
                    break


            state = container_status.state

            # -----------------------------------------------------------
            # 3) Detect init container failures early
            # -----------------------------------------------------------
            if state is None:
                log.error("wait_for_completion waiting for container", main_name)
            # -----------------------------------------------------------
            # 4) Waiting state – ALSO detect image pull issues here
            # -----------------------------------------------------------
            elif self.state_is_waiting(state):
                log.info(
                    "wait_for_completion state_is_waiting pod name %s with container name %s with id %s has status %s",
                    pod_name,
                    main_name,
                    pod.metadata.uid,
                    container_status,
                )

                waiting = state.waiting
                reason = waiting.reason if waiting else None
                message = waiting.message if waiting else None

                image_pull_reasons = {"ErrImagePull", "ImagePullBackOff", "RegistryUnavailable"}
                is_image_pull_issue = False
                if reason in image_pull_reasons:
                    is_image_pull_issue = True
                elif message and "pull" in message.lower() and "image" in message.lower():
                    is_image_pull_issue = True

                if is_image_pull_issue:
                    image_name = None
                    try:
                        spec = self._get_container_spec_by_name(pod, main_name)
                        if spec:
                            image_name = spec.image
                    except Exception:
                        pass
                    result_reason = f"Image pull failed={image_name or 'unknown'}; {reason or 'unknown'}; {message}"
                    log.info(
                        f"wait_for_completion Image pull issue pod name:{str(pod_name)}  container name: {main_name} reason: {message}" ,
                        
                    )

                    self.completion_result = self._make_failure_result(
                        pod_name,
                        reason=result_reason,
                    )
                    w.stop()
                    break
                # plain waiting – keep watching
                continue


            # -----------------------------------------------------------
            # 5) Running – follow logs, then we expect a terminated event
            # -----------------------------------------------------------
            elif self.state_is_running(state):
                log.info(
                    "wait_for_completion state_is_running pod name %s with container name %s with id %s has status %s",
                    pod_name,
                    main_name,
                    pod.metadata.uid,
                    container_status,
                )
                self.follow_logs(container_name=main_name)
                log.info("wait_for_completion pod %s follow_logs container %s", pod_name, main_name)
                # after logs return, we loop again to see the terminated state
                continue

            # -----------------------------------------------------------
            # 6) Terminated – normal success/failure from container
            # -----------------------------------------------------------
            elif self.state_is_terminated(state):
                log.info(
                    "wait_for_completion Handling terminated pod name %s with id %s",
                    pod_name,
                    pod.metadata.uid,
                )
                container = self._get_container_spec_by_name(pod, main_name)
                self._handle_completion(state, container)
                if self.should_delete_pod():
                    with PodMonitor() as monitor:
                        self.delete_pod_name(pod_name)
                        monitor.remove(pod)
                self._clear_pod()
                w.stop()
                break

            # -----------------------------------------------------------
            # 7) Anything else – make it a failure result, not an exception
            # -----------------------------------------------------------
            log.error("wait_for_completion Unexpected pod container status for %s: %s", pod_name, container_status)
            continue
            

        # -----------------------------------------------------------
        # 8) Final safeguard
        # -----------------------------------------------------------
        if self.completion_result is None:
            log.error("wait_for_completion Finished with no completion result.")
            raise IncompleteStatusException

        return self.completion_result


    def _set_pod(self, pod):
        log.info('k8s pod \'{}\' started'.format(pod.metadata.name))
        if self.pod is not None:
            raise CalrissianJobException('This client is already observing pod {}'.format(self.pod))
        self.pod = pod

    def _clear_pod(self):
        self.pod = None

    def _get_pod_field_selector(self):
        return 'metadata.name={}'.format(self.pod.metadata.name)

    @staticmethod
    def state_is_running(state):
        return state.running

    @staticmethod
    def state_is_waiting(state):
        return state.waiting

    @staticmethod
    def state_is_terminated(state):
        return state.terminated

    @staticmethod
    def get_first_or_none(container_list: List[Union[V1ContainerStatus, V1Container]]) -> Union[V1ContainerStatus, V1Container]:
        """
        Check the list. Should be 0 or 1 items. If 0, there's no container yet. If 1, there's a
        container. If > 1, there's more than 1 container and that's unexpected behavior
        :param containers_or_container_statuses: list of V1ContainerStatus or V1Container
        :return: first item if len of list is 1, None if 0, and raises CalrissianJobException if > 1
        """
        if not container_list: # None or empty list
            return None
        elif len(container_list) > 1:
            raise CalrissianJobException(
                'Expected 0 or 1 containers, found {}'.format(len(container_list), container_list))
        else:
            return container_list[0]

    def _extract_cpu_memory_requests(self, container):
        if container.resources.requests:
            return (container.resources.requests[k] for k in ['cpu','memory'])
        else:
            raise CalrissianJobException('Unable to extract CPU/memory requests, not present')

    def _extract_start_finish_times(self, state):
        """
        Extracts the started_at and finished_at timestamps from state.terminated
        :param state: V1ContainerState
        :return: (started_at, finished_at)
        """
        return (state.terminated.started_at, state.terminated.finished_at,)

    @retry_exponential_if_exception_type((ApiException, HTTPError,), log)
    def get_pod_for_name(self, pod_name):
        """
        Given a pod name return details about this pod
        :param pod_name: str: name of the pod to read data about
        :return: V1Pod
        """
        pod_name_field_selector = 'metadata.name={}'.format(pod_name)
        pod_list = self.core_api_instance.list_namespaced_pod(self.namespace, field_selector=pod_name_field_selector)
        if not pod_list.items:
            raise CalrissianJobException("Unable to find pod with name {}".format(pod_name))
        if len(pod_list.items) != 1:
            raise CalrissianJobException("Multiple pods found with name {}".format(pod_name))
        return pod_list.items[0]

    def get_current_pod(self):
        """
        Return pod details about the current pod (ie the one we are running inside of).
        Requires 'CALRISSIAN_POD_NAME' environment variable to be set.
        :return: V1Pod
        """
        pod_name = os.environ.get(POD_NAME_ENV_VARIABLE)
        if not pod_name:
            raise CalrissianJobException("Missing required environment variable ${}".format(POD_NAME_ENV_VARIABLE))
        return self.get_pod_for_name(pod_name)


class PodMonitor(object):
    """
    This class is designed to track pods submitted by KubernetesClient across different background threads,
    and provide a static cleanup() method to attempt to delete those pods on termination.

    Instances of this class are used as context manager, and acquire the shared lock.
    The add and remove methods should only be called from inside the context block while the lock is acquired.

    The static cleanup() method also acquires the lock and attempts to delete all outstanding pods.

    """
    pod_names = []
    lock = threading.Lock()

    def __enter__(self):
        PodMonitor.lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        PodMonitor.lock.release()

    # add and remove methods should be called with the lock acquired, e.g. inside PodMonitor():
    def add(self, pod):
        log.info('PodMonitor adding {}'.format(pod.metadata.name))
        PodMonitor.pod_names.append(pod.metadata.name)

    def remove(self, pod):
        # This has to look up the pod by something unique
        if pod.metadata.name in PodMonitor.pod_names:
            log.info('PodMonitor removing {}'.format(pod.metadata.name))
            PodMonitor.pod_names.remove(pod.metadata.name)
        else:
            log.warning('PodMonitor {} has already been removed'.format(pod.metadata.name))

    @staticmethod
    def cleanup():
        log.info('Starting Cleanup')
        with PodMonitor() as monitor:
            k8s_client = KubernetesClient()
            for pod_name in PodMonitor.pod_names:
                log.info('PodMonitor deleting pod {}'.format(pod_name))
                try:
                    k8s_client.delete_pod_name(pod_name)
                except Exception:
                    log.error('Error deleting pod named {}, ignoring'.format(pod_name))
            PodMonitor.pod_names = []
        log.info('Finishing Cleanup')


def delete_pods():
    PodMonitor.cleanup()
