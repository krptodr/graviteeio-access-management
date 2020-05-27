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
package io.gravitee.am.service.impl;

import io.gravitee.am.model.Application;
import io.gravitee.am.model.User;
import io.gravitee.am.model.common.Page;
import io.gravitee.am.model.uma.ResourceSet;
import io.gravitee.am.repository.management.api.ResourceSetRepository;
import io.gravitee.am.service.ApplicationService;
import io.gravitee.am.service.ResourceSetService;
import io.gravitee.am.service.UserService;
import io.gravitee.am.service.exception.ResourceSetNotFoundException;
import io.gravitee.am.service.model.NewResourceSet;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Alexandre FARIA (contact at alexandrefaria.net)
 * @author GraviteeSource Team
 */
@Component
public class ResourceSetServiceImpl implements ResourceSetService {

    private final Logger LOGGER = LoggerFactory.getLogger(ResourceSetServiceImpl.class);

    @Lazy
    @Autowired
    private ResourceSetRepository repository;

    @Autowired
    private UserService userService;

    @Autowired
    private ApplicationService applicationService;

    @Override
    public Single<Page<ResourceSet>> findByDomainAndClient(String domain, String client, int page, int size) {
        LOGGER.debug("Listing resource set for domain {} and client {}", domain, client);
        return repository.findByDomainAndClient(domain, client, page, size);
    }

    @Override
    public Single<List<ResourceSet>> listByDomainAndClientAndUser(String domain, String client, String userId) {
        LOGGER.debug("Listing resource set for resource owner {} and client {}", userId, client);
        return repository.findByDomainAndClientAndUser(domain, client, userId);
    }

    @Override
    public Single<List<ResourceSet>> findByDomainAndClientAndUserAndResources(String domain, String client, String userId, List<String> resourceIds) {
        LOGGER.debug("Getting resource set {} for resource owner {} and client {} and resources {}", resourceIds, userId, client, resourceIds);
        return repository.findByDomainAndClientAndUserAndResource(domain, client, userId, resourceIds);
    }

    @Override
    public Maybe<ResourceSet> findByDomainAndClientResource(String domain, String client, String resourceId) {
        LOGGER.debug("Getting resource set {} for client {} and resource {}", resourceId, client, resourceId);
        return repository.findById(resourceId)
                .switchIfEmpty(Maybe.error(new ResourceSetNotFoundException(resourceId)))
                .map(resourceSet -> {
                    if (!domain.equals(resourceSet.getDomain())) {
                        throw new ResourceSetNotFoundException(resourceId);
                    }
                    if (!client.equals(resourceSet.getClientId())) {
                        throw new ResourceSetNotFoundException(resourceId);
                    }
                    return resourceSet;
                });
    }

    @Override
    public Maybe<ResourceSet> findByDomainAndClientAndUserAndResource(String domain, String client, String userId, String resourceId) {
        LOGGER.debug("Getting resource set {} for resource owner {} and client {} and resource {}", resourceId, userId, client, resourceId);
        return repository.findByDomainAndClientAndUserAndResource(domain, client, userId, resourceId);
    }

    @Override
    public Single<Map<String, Map<String, Object>>> getMetadata(List<ResourceSet> resources) {
        if (resources == null || resources.isEmpty()) {
            return Single.just(Collections.emptyMap());
        }

        List<String> userIds = resources.stream().filter(resource -> resource.getUserId() != null).map(ResourceSet::getUserId).distinct().collect(Collectors.toList());
        List<String> appIds = resources.stream().filter(resource -> resource.getClientId() != null).map(ResourceSet::getClientId).distinct().collect(Collectors.toList());

        return Single.zip(userService.findByIdIn(userIds), applicationService.findByIdIn(appIds), (users, apps) -> {
            Map<String, Map<String, Object>> metadata = new HashMap<>();
            metadata.put("users", users.stream().collect(Collectors.toMap(User::getId, this::convert)));
            metadata.put("applications", apps.stream().collect(Collectors.toMap(Application::getId, this::convert)));
            return metadata;
        });
    }

    @Override
    public Single<ResourceSet> create(NewResourceSet resourceSet, String domain, String client, String userId) {
        LOGGER.debug("Creating resource set for resource owner {} and client {}", userId, client);
        ResourceSet toCreate = new ResourceSet();
        toCreate.setResourceScopes(resourceSet.getResourceScopes())
                .setDescription(resourceSet.getDescription())
                .setIconUri(resourceSet.getIconUri())
                .setName(resourceSet.getName())
                .setType(resourceSet.getType())
                .setDomain(domain)
                .setClientId(client)
                .setUserId(userId)
                .setCreatedAt(new Date())
                .setUpdatedAt(toCreate.getCreatedAt());

        return repository.create(toCreate);
    }

    @Override
    public Single<ResourceSet> update(NewResourceSet resourceSet, String domain, String client, String userId, String resourceId) {
        LOGGER.debug("Updating resource set id {} for resource owner {} and client {}", resourceId, userId, client);
        return findByDomainAndClientAndUserAndResource(domain, client, userId, resourceId)
                .switchIfEmpty(Maybe.error(new ResourceSetNotFoundException(resourceId)))
                .flatMapSingle(Single::just)
                .map(toUpdate -> resourceSet.update(toUpdate))
                .map(toUpdate -> toUpdate.setUpdatedAt(new Date()))
                .flatMap(repository::update);
    }

    @Override
    public Completable delete(String domain, String client, String userId, String resourceId) {
        LOGGER.debug("Deleting resource set id {} for resource owner {} and client {}", resourceId, userId, client);
        return findByDomainAndClientAndUserAndResource(domain, client, userId, resourceId)
                .switchIfEmpty(Maybe.error(new ResourceSetNotFoundException(resourceId)))
                .flatMapCompletable(found -> repository.delete(resourceId));
    }

    private User convert(User user) {
        User resourceOwner = new User();
        resourceOwner.setId(user.getId());
        resourceOwner.setDisplayName(user.getDisplayName());
        return resourceOwner;
    }

    private Application convert(Application application) {
        Application client = new Application();
        client.setId(application.getId());
        client.setName(application.getName());
        return client;
    }
}
