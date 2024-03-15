export type Language = 'English' | 'French'

export type Movie = {
    movieId: number;
    genre_ids: number[];
    original_language: Language;
    overview: string;
    popularity: number;
    release_date: string;
    title: string
    video: boolean;
    vote_average: number;
    vote_count: number
}

export type MovieCast = {
    movieId: number;
    actorName: string;
    roleName: string;
    roleDescription: string;
};

export type MovieCastMemberQueryParams = {
    movieId: string;
    actorName?: string;
    roleName?: string
}

export type MovieQueryParams = {
    cast?: boolean;
}

export type MovieAward = {
    movieId: number;
    awardBody: string;
    numAwards: number;
    awardDescription: string;
};

export type MovieCrewRole = {
    movieId: number;
    crewRole: string;
    names: string;
};
