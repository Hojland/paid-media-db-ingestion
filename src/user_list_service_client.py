# -*- coding: utf-8 -*-
#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Accesses the google.ads.googleads.v4.services UserListService API."""

import pkg_resources
import warnings

from google.oauth2 import service_account
import google.api_core.gapic_v1.client_info
import google.api_core.gapic_v1.config
import google.api_core.gapic_v1.method
import google.api_core.gapic_v1.routing_header
import google.api_core.grpc_helpers
import google.api_core.path_template

from google.ads.google_ads.v4.services import user_list_service_client_config
from google.ads.google_ads.v4.services.transports import user_list_service_grpc_transport
from google.ads.google_ads.v4.proto.services import user_list_service_pb2



_GAPIC_LIBRARY_VERSION = pkg_resources.get_distribution(
    'google-ads',
).version


class UserListServiceClient(object):
    """Service to manage user lists."""

    SERVICE_ADDRESS = 'googleads.googleapis.com:443'
    """The default address of the service."""

    # The name of the interface for this client. This is the key used to
    # find the method configuration in the client_config dictionary.
    _INTERFACE_NAME = 'google.ads.googleads.v4.services.UserListService'


    @classmethod
    def user_list_path(cls, customer, user_list):
        """Return a fully-qualified user_list string."""
        return google.api_core.path_template.expand(
            'customers/{customer}/userLists/{user_list}',
            customer=customer,
            user_list=user_list,
        )

    def __init__(self, client):
        """
            client (GoogleAdsClient)
        """

        self.client = client
        self._method_configs = user_list_service_client_config.config
        self._inner_api_calls = {}
        self._client_info = google.api_core.gapic_v1.client_info.ClientInfo(
                gapic_version=_GAPIC_LIBRARY_VERSION,
            )
        self.transport = user_list_service_grpc_transport.UserListServiceGrpcTransport(
            address=self.SERVICE_ADDRESS,
            channel=channel,
            credentials=credentials,
        )

    # Service calls
    def get_user_list(
            self,
            resource_name,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Returns the requested user list.
        Args:
            resource_name (str): Required. The resource name of the user list to fetch.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will not
                be retried.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.
        Returns:
            A :class:`~google.ads.googleads_v4.types.UserList` instance.
        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'get_user_list' not in self._inner_api_calls:
            self._inner_api_calls['get_user_list'] = google.api_core.gapic_v1.method.wrap_method(
                self.client.get_user_list,
                default_retry=self._method_configs['GetUserList'].retry,
                default_timeout=self._method_configs['GetUserList'].timeout,
                client_info=self._client_info,
            )

        request = user_list_service_pb2.GetUserListRequest(
            resource_name=resource_name,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('resource_name', resource_name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['get_user_list'](request, retry=retry, timeout=timeout, metadata=metadata)

    def mutate_user_lists(
            self,
            customer_id,
            operations,
            partial_failure=None,
            validate_only=None,
            retry=google.api_core.gapic_v1.method.DEFAULT,
            timeout=google.api_core.gapic_v1.method.DEFAULT,
            metadata=None):
        """
        Creates or updates user lists. Operation statuses are returned.
        Args:
            customer_id (str): Required. The ID of the customer whose user lists are being modified.
            operations (list[Union[dict, ~google.ads.googleads_v4.types.UserListOperation]]): Required. The list of operations to perform on individual user lists.
                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.ads.googleads_v4.types.UserListOperation`
            partial_failure (bool): If true, successful operations will be carried out and invalid
                operations will return errors. If false, all operations will be carried
                out in one transaction if and only if they are all valid.
                Default is false.
            validate_only (bool): If true, the request is validated but not executed. Only errors are
                returned, not results.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will not
                be retried.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.
        Returns:
            A :class:`~google.ads.googleads_v4.types.MutateUserListsResponse` instance.
        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if 'mutate_user_lists' not in self._inner_api_calls:
            self._inner_api_calls['mutate_user_lists'] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.mutate_user_lists,
                default_retry=self._method_configs['MutateUserLists'].retry,
                default_timeout=self._method_configs['MutateUserLists'].timeout,
                client_info=self._client_info,
            )

        request = user_list_service_pb2.MutateUserListsRequest(
            customer_id=customer_id,
            operations=operations,
            partial_failure=partial_failure,
            validate_only=validate_only,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [('customer_id', customer_id)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(routing_header)
            metadata.append(routing_metadata)

        return self._inner_api_calls['mutate_user_lists'](request, retry=retry, timeout=timeout, metadata=metadata)