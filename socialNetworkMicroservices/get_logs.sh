for service in UserService SocialGraphService UrlShortenService PostStorageService HomeTimelineService ComposePostService UserTimelineService UserMentionService UniqueIdService TextService MediaService; do
    echo "$service"
    docker logs $(docker container ls | grep $service | awk '{print $1}')
done
