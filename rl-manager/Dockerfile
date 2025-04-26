FROM python:3.13-alpine

WORKDIR /mgr

RUN pip3 install --upgrade pip==24.3.1 && pip3 install stable-baselines3==2.4.0 sb3-contrib==2.4.0 \
    tensorboard==2.18.0 tqdm==4.67.1 rich==13.9.4 pyyaml==6.0.2 pycurl==7.45.3 scikit-learn==1.5.2

COPY rl-manager/gym_cloudsimplus gym_cloudsimplus

RUN pip3 install -e gym_cloudsimplus
RUN apt update \
    && apt purge --auto-remove -yqq \
    && apt autoremove -yqq --purge \
    && apt clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base