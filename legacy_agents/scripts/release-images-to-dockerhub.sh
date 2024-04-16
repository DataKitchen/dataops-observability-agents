#!/usr/bin/env bash

function exit_script() {
	local ret="${2:-1}"
	echo "$1" >&2
	exit "${ret}"
}

# Desired image tag. We've been using a v# semantic.
IMAGE_TAG=
# The namespace for the image.
NAMESPACE=
DOCKER_LOGIN=
DOCKER_USERNAME=
BUILD_ONLY=false
RELEASE_ONLY=false
TARGET_IMAGES=()

function print_help() {
	printf '%s\n' "A script for pushing new version of the images to Dockerhub."
	printf 'Usage: %s [-t|--image-tag <arg>] [-n|--namespace <arg>] [-l|--docker-login <arg>] [-u|--docker-password <arg>] <image-name>\n' "$0"
	printf '\t%s\n' "-t, --image-tag: The version tag. We use v#."
	printf '\t%s\n' "-r, --release-only: Only do the docker tag and push steps. (default false)."
	printf '\t%s\n' "-b, --build-only: Only do the docker build and tag steps. (default false)."
	printf '\t%s\n' "-n, --namespace: the namespace., e.g., will push \$namespace/image-name:\$image-tag"
	printf '\t%s\n' "-l, --docker-login: Docker password. Acquire from DevOps or 1Password"
	printf '\t%s\n' "-u, --docker-username: Docker username. Acquire from DevOps or 1Password."
	printf '\t%s\n\n' "-h, --help: Prints help"
	printf '\t%s\n' "<Positional>: The name of the image being targeted for released. Note: all valid names"
	printf '\t%s\n' "have an associated docker file: e.g.,/deploy/docker/<name>.dockerfile\n"
}

function check_argument_is_set() {
	arg_name="$1"
	arg_value="$2"
	if [ -z "$arg_value" ]; then
		exit_script "FATAL ERROR: Missing $arg_name"
	fi
}

function check_passed_args_count() {
	local num_args="${1}"
	if [ "${num_args}" -lt 1 ]; then
		exit_script "FATAL ERROR: Missing at least one image name." 1
	fi
}

function docker_build() {
	local image_name="$1"
	local tag="${2}"

  image_tag="${image_name}:$tag"
	echo "Building ${image_name}..."
	if ! docker build . \
	   --build-arg tag="$tag" \
	   --no-cache \
	   -t "$NAMESPACE/$image_tag" \
	   -t "$image_tag" \
	   -f "./deploy/docker/${image_name}.dockerfile"; then
		exit_script "Docker Build failed!" 1
	fi
}

function docker_login() {
	echo "Logging in to dockerhub.io..."
	check_argument_is_set DOCKER_LOGIN "$DOCKER_LOGIN"
	check_argument_is_set DOCKER_USERNAME "$DOCKER_USERNAME"
	if ! docker login -u "$DOCKER_USERNAME" -p "$DOCKER_LOGIN"; then
		exit_script "FATAL ERROR: failed to log in to DockerHub." 1
	fi
}

function docker_tag() {
	local image_name="$1"
	local version_tag="$2"

	# shellcheck disable=SC2066
	for tag in "$version_tag"; do
		if ! docker tag "$image_name:$tag" "$NAMESPACE/$image_name:$tag"; then
			exit_script "Tagging with '$tag' failed!" 1
		fi
	done
}

function docker_push() {
	local image_name="$1"
	local version_tag="$2"

	# shellcheck disable=SC2066
	for tag in "$version_tag"; do
		if ! docker push "$NAMESPACE/$image_name:$tag"; then
			exit_script "pushing tag '$tag' failed!" 1
		fi
	done
}

function parse_commandline() {
	_positionals_count=0
	while test $# -gt 0; do
		arg="$1"
		case "$arg" in
		-b | --build-only)
			BUILD_ONLY=true
			shift
			;;
		-r | --release-only)
			RELEASE_ONLY=true
			shift
			;;
		-t | --image-tag)
			test $# -lt 2 && exit_script "Missing value for the argument '$arg'." 1
			IMAGE_TAG="$2"
			shift
			;;
		--image-tag=*)
			IMAGE_TAG="${arg##--image-tag=}"
			;;
		-t*)
			IMAGE_TAG="${arg##-t}"
			;;
		-n | --namespace)
			test $# -lt 2 && exit_script "Missing value for the argument '$arg'." 1
			NAMESPACE="$2"
			shift
			;;
		--namespace=*)
			NAMESPACE="${arg##--namespace=}"
			;;
		-n*)
			NAMESPACE="${arg##-n}"
			;;
		-l | --docker-login)
			test $# -lt 2 && exit_script "Missing value for the argument '$arg'." 1
			DOCKER_LOGIN="$2"
			shift
			;;
		--docker-login=*)
			DOCKER_LOGIN="${arg##--docker-login=}"
			;;
		-u | --docker-username)
			test $# -lt 2 && exit_script "Missing value for the argument '$arg'." 1
			DOCKER_USERNAME="$2"
			shift
			;;
		--docker-username=*)
			DOCKER_USERNAME="${arg##--docker-username=}"
			;;
		-h | --help)
			print_help
			exit_script "exiting..." 0
			;;
		*)
			_last_image="$1"
			_positionals_count=$((_positionals_count + 1))
			TARGET_IMAGES+=("$_last_image")
			;;
		esac
		shift
	done

	check_passed_args_count "$_positionals_count"
	check_argument_is_set IMAGE_TAG "$IMAGE_TAG"

	# check all images exist
	for IMAGE_NAME in "${TARGET_IMAGES[@]}"; do
		if [ ! -f "$(pwd)/deploy/docker/$IMAGE_NAME.dockerfile" ]; then
			exit_script "Dockerfile does not exist: $(pwd)/deploy/docker/$IMAGE_NAME.dockerfile" 1
		fi
	done
}

parse_commandline "$@"
for IMAGE_NAME in "${TARGET_IMAGES[@]}"; do
	if [[ "${RELEASE_ONLY}" = true ]]; then
		docker_login
		docker_tag "$IMAGE_NAME" "$IMAGE_TAG"
		docker_push "$IMAGE_NAME" "$IMAGE_TAG"
		echo "Release Only."
		continue
	fi

	docker_build dk-obs-agent-deps "$IMAGE_TAG"
	docker_build dk-obs-agent-base "$IMAGE_TAG"
	docker_build "$IMAGE_NAME" "$IMAGE_TAG"
	docker_tag "$IMAGE_NAME" "$IMAGE_TAG"

	if [[ "$BUILD_ONLY" = true ]]; then
		echo "Build Only."
		continue
	fi

	docker_login
	docker_push "$IMAGE_NAME" "$IMAGE_TAG"
done

echo "Complete!"
