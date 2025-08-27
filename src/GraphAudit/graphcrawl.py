import os
import asyncio
from .graphdata import GraphData
from msgraph import GraphServiceClient
from azure.identity import InteractiveBrowserCredential, TokenCachePersistenceOptions, AuthenticationRecord
from kiota_abstractions.base_request_configuration import RequestConfiguration
from msgraph.generated.models.o_data_errors.o_data_error import ODataError
from msgraph.generated.service_principals.service_principals_request_builder import ServicePrincipalsRequestBuilder
from msgraph.generated.service_principals.item.app_role_assignments.app_role_assignments_request_builder import AppRoleAssignmentsRequestBuilder
from msgraph.generated.applications.applications_request_builder import ApplicationsRequestBuilder
from msgraph.generated.service_principals.item.app_role_assigned_to.app_role_assigned_to_request_builder import AppRoleAssignedToRequestBuilder
from msgraph.generated.service_principals.item.oauth2_permission_grants.oauth2_permission_grants_request_builder import Oauth2PermissionGrantsRequestBuilder
from msgraph.generated.service_principals.item.service_principal_item_request_builder import ServicePrincipalItemRequestBuilder
from msgraph.generated.service_principals.item.member_of.member_of_request_builder import MemberOfRequestBuilder
import logging
from .log import log_init
import pandas as pd
import httpx


# Azure CLI client ID (public, multi-tenant)
CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
# Default scope for Microsoft Graph
SCOPES = ["https://graph.microsoft.com/.default"]


class GraphException(Exception):
    def __init__(self, message, *args, **kwargs):
        logger = logging.getLogger(__name__)
        logger.error("%s", message, exc_info=True)
        super().__init__(message, *args, **kwargs)

class GraphCrawler:
    def __init__(self, graph_data, debug = 0, batch_size = 250, use_cache = False):
        
        self._logger       = log_init(__name__)
        self._graph_data   = graph_data
        self._debug        = debug
        self._batch_size   = batch_size
        self._graph_client = None
        self._semaphore    = asyncio.Semaphore(5)
        self._use_cache    = use_cache
        
    async def __aenter__(self):
        print(f"Use cache: {self._use_cache}")
        await self._authenticate(use_cache=self._use_cache)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._graph_client and hasattr(self._graph_client, 'request_adapter'):
            try:
                # Close the underlying HTTP client via async close method
                if hasattr(self._graph_client.request_adapter, 'get_http_client'):
                    client = self._graph_client.request_adapter.get_http_client()
                    if hasattr(client, 'aclose'):
                        await client.aclose()
            except Exception as e:
                self._logger.error(f"Error closing HTTP client: {e}")


    async def _authenticate(self, client_id=CLIENT_ID, use_cache=False):
        try:
            credential = None
            if use_cache:
                cache_path = os.path.expanduser(".token_cache")
                auth_record_path = os.path.expanduser(".auth_record_cache")
            
                cache_options = TokenCachePersistenceOptions(
                    name=cache_path, 
                    allow_unencrypted_storage=True
                )

                if os.path.exists(auth_record_path):
                    with open(auth_record_path, 'r') as fp:
                        record_json = fp.read()
                        record = AuthenticationRecord.deserialize(record_json)

                    credential = InteractiveBrowserCredential(
                        client_id=client_id,
                        cache_persistence_options=cache_options, 
                        authentication_record=record
                    )
                else:
                    credential = InteractiveBrowserCredential(
                        client_id=client_id,
                        cache_persistence_options=cache_options
                    )

                    record = await asyncio.get_event_loop().run_in_executor(
                        None, credential.authenticate
                    )
                    record_json = record.serialize()
                    with open(auth_record_path, 'w') as auth_out:
                        auth_out.write(record_json)
            else:
                credential = InteractiveBrowserCredential(
                    client_id=client_id
                )

            self._graph_client = GraphServiceClient(
                credentials=credential, 
                scopes=SCOPES
            )
            
        except Exception as e:
            raise GraphException(f"Error authenticating credential: {e}")



    async def _paginate_with_retry(
        self, 
        client, 
        initial_response, 
        response_type,
        max_retries = 3
    ):
        if not initial_response or not initial_response.value:
            return

        for item in initial_response.value:
            yield item

        next_link = initial_response.odata_next_link
        retry_count = 0

        while next_link:
            try:
                async with self._semaphore: 
                    request_info = client.to_get_request_information()
                    request_info.url_template = next_link
                    request_info.path_parameters = {}

                    response = await client.request_adapter.send_async(
                        request_info,
                        response_type,
                        error_map={"4XX": ODataError, "5XX": ODataError}
                    )

                    if response and response.value:
                        for item in response.value:
                            yield item
                        next_link = response.odata_next_link
                        retry_count = 0  # Reset retry count on success
                    else:
                        next_link = None
                        
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
                retry_count += 1
                if retry_count <= max_retries:
                    wait_time = min(2 ** retry_count, 30)  # Exponential backoff, max 30s
                    self._logger.warning(f"Connection error, retrying in {wait_time}s (attempt {retry_count}/{max_retries}): {e}")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self._logger.error(f"Max retries exceeded for pagination: {e}")
                    raise GraphException(f"Connection failed after {max_retries} retries: {e}")
            except Exception as e:
                self._logger.error(f"Unexpected error during pagination: {e}")
                raise GraphException(f"Pagination error: {e}")
            



    async def fetch(self):
        try:
            self._logger.info("[*] Starting collection: This might take a few hours depending on the size of your Entra-ID Directory ☕️")
            self._logger.info("[*] Starting to fetch applications...")
            df = await self.fetch_applications()
            if not df.empty:
                self._graph_data.store_table('applications', df)
                #self._logger.info(f"[+] Stored {len(df)} applications")

            self._logger.info("[*] Starting to fetch service principals...")
            df_list = await self.fetch_service_principals()
            tables = (
                'service_principals', 
                'app_role_assignments', 
                'app_role_assigned_to', 
                'app_roles', 
                'sp_oauth_grants', 
                'sp_member_of' 
            )   
            for table, df in zip(tables, df_list):
                if not df.empty:
                    self._graph_data.store_table(table, df)
                    self._logger.info(f"[+] Stored {len(df)} records in {table}")
                    
        except Exception as e:
            self._logger.error(f"Error fetching data: {e}")
            raise



    async def fetch_service_principals(self):
        sp_list = []
        member_of_list = []
        app_roles_list = []
        oauth_grants_list = []
        app_role_assignment_list = []
        app_role_assigned_to_list = []

        try:
            query_params = ServicePrincipalsRequestBuilder.\
                ServicePrincipalsRequestBuilderGetQueryParameters(top=999)
            request_config = RequestConfiguration(query_parameters=query_params)

            response = await self._graph_client.service_principals.get(
                request_configuration=request_config
            )

            counter       = 0
            batch_counter = 0

            page_response = self._paginate_with_retry(
                self._graph_client.service_principals, 
                response, 
                type(response)
            )
            
            tasks = []
            async for sp in page_response:
                sp_list.append(self._graph_data.kiota_to_json(sp))

                # Handle app roles
                if sp.app_roles:
                    for role in sp.app_roles:
                        role_data = self._graph_data.kiota_to_json(role)
                        role_data['service_principal_id'] = sp.id
                        app_roles_list.append(role_data)

                # Create task for subresources
                task = self.fetch_sp_subresources_batch(sp.id)
                tasks.append(task)
                
                counter       += 1
                batch_counter += 1
                
                # Process in batches
                if batch_counter >= self._batch_size or (self._debug and counter >= self._debug):
                    self._logger.info(f"[*] Processing batch of {len(tasks)} service principals...")
                    results_batch = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for result in results_batch:
                        if isinstance(result, Exception):
                            self._logger.error(f"Error in batch processing: {result}")
                            continue
                            
                        app_role_assignment_list.extend(result[0])
                        app_role_assigned_to_list.extend(result[1])
                        oauth_grants_list.extend(result[2])
                        member_of_list.extend(result[3])
                    
                    tasks = []
                    batch_counter = 0
                    
                    # Delay between batches
                    await asyncio.sleep(0.5)
                
                if self._debug and counter >= self._debug:
                    break
            
            # Process remaining tasks
            if tasks:
                self._logger.info(f"[*] Processing final batch of {len(tasks)} service principals...")
                results_batch = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results_batch:
                    if isinstance(result, Exception):
                        self._logger.error(f"Error in final batch processing: {result}")
                        continue
                        
                    app_role_assignment_list.extend(result[0])
                    app_role_assigned_to_list.extend(result[1])
                    oauth_grants_list.extend(result[2])
                    member_of_list.extend(result[3])
                
        except Exception as e:
            raise GraphException(f"MS Graph API error fetching ServicePrincipals: {str(e)}")
    
        return (
            pd.DataFrame(sp_list), 
            pd.DataFrame(app_role_assignment_list), 
            pd.DataFrame(app_role_assigned_to_list),
            pd.DataFrame(app_roles_list), 
            pd.DataFrame(oauth_grants_list), 
            pd.DataFrame(member_of_list)
        )
    

    async def fetch_sp_subresources_batch(self, sp_id):
        try:
            results = await asyncio.gather(
                self.fetch_sp_subresource_with_retry(
                    'app_role_assignments',
                    AppRoleAssignmentsRequestBuilder.\
                        AppRoleAssignmentsRequestBuilderGetQueryParameters,
                    sp_id
                ),
                self.fetch_sp_subresource_with_retry(
                    'app_role_assigned_to',
                    AppRoleAssignedToRequestBuilder.\
                        AppRoleAssignedToRequestBuilderGetQueryParameters,
                    sp_id
                ),
                self.fetch_sp_subresource_with_retry(
                    'oauth2_permission_grants',
                    Oauth2PermissionGrantsRequestBuilder.\
                        Oauth2PermissionGrantsRequestBuilderGetQueryParameters,
                    sp_id
                ),
                self.fetch_sp_subresource_with_retry(
                    'member_of',
                    MemberOfRequestBuilder.\
                        MemberOfRequestBuilderGetQueryParameters,
                    sp_id
                ),
                return_exceptions=True
            )
            
            # Handle any exceptions in the results
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self._logger.error(f"Error fetching subresource {i} for SP {sp_id}: {result}")
                    processed_results.append([]) 
                else:
                    processed_results.append(result)
            
            return processed_results
            
        except Exception as e:
            self._logger.error(f"Error in batch subresource fetch for SP {sp_id}: {e}")
            return [[], [], [], []]


    async def fetch_applications(self):
        app_list = []
        try:
            query_params = ApplicationsRequestBuilder.\
                ApplicationsRequestBuilderGetQueryParameters(top=999)
            
            request_config = RequestConfiguration(query_parameters=query_params)
            
            response = await self._graph_client.applications.get(
                request_configuration=request_config
            )

            async for app in self._paginate_with_retry(
                self._graph_client.applications, 
                response, 
                type(response)
            ):
                app_list.append(self._graph_data.kiota_to_json(app))
           
        except Exception as e:
            raise GraphException(f"MS Graph API error fetching Applications: {str(e)}")
    
        return pd.DataFrame(app_list)
    


    async def fetch_sp_subresource_with_retry(
        self, 
        resource_name,
        builder, 
        sp_id,
        max_retries = 3
    ):  
        for attempt in range(max_retries + 1):
            try:
                async with self._semaphore:
                    results_list = []
                    
                    query_params = builder(top=999)
                    request_config = RequestConfiguration(query_parameters=query_params)
                      
                    sp_obj = self._graph_client.service_principals.\
                        by_service_principal_id(sp_id)
                    resource_path = getattr(sp_obj, resource_name)

                    response = await resource_path.get(request_configuration=request_config)
                    
                    async for obj in self._paginate_with_retry(
                        resource_path, 
                        response, 
                        type(response)
                    ):
                        obj_data = self._graph_data.kiota_to_json(obj)
                        obj_data['service_principal_id'] = sp_id 
                        results_list.append(obj_data)

                    return results_list
                    
            except AttributeError:
                raise GraphException(f"No such resource: {resource_name} on service principal object")
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
                if attempt < max_retries:
                    wait_time = min(2 ** attempt, 30)  # Exponential backoff
                    self._logger.error(f"Connection error fetching {resource_name} for SP {sp_id}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self._logger.error(f"Max retries exceeded for {resource_name} on SP {sp_id}: {e}")
                    return [] 
            except Exception as e:
                self._logger.error(f"Error fetching {resource_name} for SP {sp_id}: {e}")
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    return []
        
        return []