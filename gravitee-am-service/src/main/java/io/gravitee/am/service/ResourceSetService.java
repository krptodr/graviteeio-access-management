/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.am.service;

import io.gravitee.am.model.common.Page;
import io.gravitee.am.model.uma.ResourceSet;
import io.gravitee.am.service.model.NewResourceSet;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Single;

import java.util.List;
import java.util.Map;

/**
 * @author Alexandre FARIA (contact at alexandrefaria.net)
 * @author GraviteeSource Team
 */
public interface ResourceSetService {

    Single<Page<ResourceSet>> findByDomainAndClient(String domain, String client, int page, int sizes);
    Single<List<ResourceSet>> listByDomainAndClientAndUser(String domain, String client, String userId);
    Single<List<ResourceSet>> findByDomainAndClientAndUserAndResources(String domain, String client, String userId, List<String> resourceIds);
    Maybe<ResourceSet> findByDomainAndClientAndUserAndResource(String domain, String client, String userId, String resourceId);
    Maybe<ResourceSet> findByDomainAndClientResource(String domain, String client, String resourceId);
    Single<Map<String, Map<String, Object>>> getMetadata(List<ResourceSet> resources);
    Single<ResourceSet> create(NewResourceSet resourceSet, String domain, String client, String userId);
    Single<ResourceSet> update(NewResourceSet resourceSet, String domain, String client, String userId, String resourceId);
    Completable delete(String domain, String client, String userId, String resourceId);
}
